#! /usr/bin/env python3.3
"""WSGI/CGI script in Python for interfacing with Git HTTP backend."""

import functools
import http.client
import logging
import os
import subprocess
import sys
import urllib.parse
import wsgiref.handlers
import wsgiref.simple_server
import wsgiref.util

# Ensure the system path includes our modules.
try:
    import p4gf_version_3
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import p4gf_version_3

import p4gf_env_config  # pylint: disable=unused-import
import p4gf_atomic_lock
import p4gf_const
import p4gf_http_common
from p4gf_l10n import _, NTR, log_l10n
import p4gf_lfs_http_server
import p4gf_log
import p4gf_proc
from p4gf_profiler import with_timer
import p4gf_server_common
import p4gf_tempfile
import p4gf_util

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_http_server")


class GitHttpServer(p4gf_http_common.HttpServer):

    """HttpServer subclass for Git over HTTP."""

    def __init__(self, environ, input_file):
        """Initialize an instance of GitHttpServer.

        :param dict environ: WSGI environment
        :param str input_file: path to file containing request data.

        """
        super(GitHttpServer, self).__init__(environ)
        self._input_name = input_file

    def _check_push_payload(self):
        """Check if this request is a push with a payload."""
        # The Git client sends multiple requests and distinguishing the
        # actual push payload from the other requests is tricky. It may
        # either be a chunked request or have a non-trivial content-length.
        # Otherwise it is a preparatory request.
        if self._environ['REQUEST_METHOD'] != 'POST':
            return
        if self.command != p4gf_server_common.CMD_GIT_RECEIVE_PACK:
            return
        if 'HTTP_TRANSFER_ENCODING' in self._environ:
            chunked = self._environ['HTTP_TRANSFER_ENCODING'] == 'chunked'
        else:
            chunked = False
        if 'CONTENT_LENGTH' in self._environ:
            sizable = int(self._environ['CONTENT_LENGTH']) > 100
        else:
            sizable = False
        if not chunked and not sizable:
            return
        LOG.debug2('GitHttpServer receiving a push payload')
        self._environ['push_payload'] = '1'

    def before(self):
        """setup user, command, etc."""
        super(GitHttpServer, self).before()
        self.check_required_params()
        self.get_repo_name_git()
        self.get_user()
        self._get_command()
        self._check_push_payload()
        self.git_caller = functools.partial(_call_git, self._input_name, self._environ)

    def push_received(self):
        """Push payload received based on several heuristics."""
        return 'push_payload' in self._environ

    def _get_command(self):
        """Retrieve the Git command from the request environment.

        Determine if this is a pull or push operation, returning the
        canonical git command (i.e. git-upload-pack or git-receive-pack).
        Returns None if the command does not match what is expected, or is
        not provided.

        """
        if not self.command:
            cmd = None
            method = self._environ["REQUEST_METHOD"]
            if method == 'POST':
                # we expect the git command to be at the end
                cmd = os.path.basename(self.environ['PATH_INFO'])
            elif method == 'GET':
                qs = self._environ.get('QUERY_STRING')
                if qs:
                    params = urllib.parse.parse_qs(qs)
                    cmd = params.get('service')[0]
            LOG.debug('_get_command() retrieved %s command', cmd)
            # check for the two allowable commands/services; anything else is wrong
            if cmd == 'git-upload-pack' or cmd == 'git-receive-pack':
                self.command = cmd


class OutputSink(object):

    """Context manager that redirects standard output and error streams.

    Redirects to a temporary file, which is automatically deleted.
    """

    def __init__(self):
        """Initialize an instance of OutputSink."""
        self.__temp = None
        self.__stdout = None
        self.__stderr = None

    def __enter__(self):
        """Enter the context."""
        if not self.__temp:
            self.__temp = p4gf_tempfile.new_temp_file(
                mode='w+', encoding='UTF-8', prefix='http-output-', delete=False)
            self.__stdout = sys.stdout
            self.__stderr = sys.stderr
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout = self.__temp
            sys.stderr = self.__temp
            LOG.debug("stdout/stderr redirecting to %s...", self.__temp.name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context."""
        if self.__temp:
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout = self.__stdout
            sys.stderr = self.__stderr
            self.__temp.close()
            try:
                os.unlink(self.__temp.name)
            except OSError:
                LOG.warning("OutputSink failed to delete file %s", self.__temp.name)
            self.__temp = None
            LOG.debug("stdout/stderr redirection terminated")
        return False

    def readall(self):
        """Return the contents of the temporary file as bytes.

        Return an empty bytes array if the file has been closed.
        """
        if not self.__temp:
            return bytes()
        self.__temp.seek(0)
        txt = self.__temp.read()
        return txt.encode('UTF-8')


_REQUIRED_ENVARS = (
    'PATH',
    'PATH_INFO',
    'QUERY_STRING',
    'REMOTE_ADDR',
    'REMOTE_USER',
    'REQUEST_METHOD'
    )
_OPTIONAL_ENVARS = (
    'CONTENT_LENGTH',
    'CONTENT_TYPE',
    'HTTP_ACCEPT',
    'HTTP_CONTENT_ENCODING',
    'HTTP_PRAGMA',
    'LANG',
    'P4CONFIG',
    # 'P4PASSWD', -- if nothing else works...
    'P4PORT',
    'P4USER',
    p4gf_const.P4GF_TEST_LOG_CONFIG_PATH,
    'P4GF_P4D_VERSION_FORCE_ACCEPTABLE',
    p4gf_const.P4GF_TEST_UUID_SEQUENTIAL,
    p4gf_const.P4GF_TEST_LFS_FILE_MAX_SECONDS_TO_KEEP,
    p4gf_const.P4GF_ENV_NAME,
    'PYTHONPATH',
    p4gf_const.P4GF_LOG_UUID,
    p4gf_const.P4GF_LOG_REPO
    )


@with_timer('git backend')
def _call_git(input_name, environ, ctx):
    """Invoke git http-backend with the appropriate environment.

    Use the function given by environ['proc.caller'] to invoke the child process
    such that its output will be directed back to the client.

    Arguments:
        input_name -- file path of input for git-http-backend.
        environ -- environment variables.
        ctx -- context object.

    Returns the exit code of the git-http-backend process.

    """
    # Set up an environment not only for git-http-backend but also for
    # our pre-receive hook script, which needs to know how to connect
    # to Perforce and how it might need to log messages. Keep in mind
    # that most web servers are run as users that do not have a home
    # directory (and likewise are lacking shell initialization files).
    env = dict()
    # Set specific values for some of the parameters.
    env['GIT_HTTP_EXPORT_ALL'] = '1'
    env['GIT_PROJECT_ROOT'] = ctx.repo_dirs.GIT_DIR
    env['HOME'] = environ.get('HOME', os.path.expanduser('~'))
    env[p4gf_const.P4GF_AUTH_P4USER] = environ['REMOTE_USER']
    if ctx.foruser:
        env[p4gf_const.P4GF_FORUSER] = ctx.foruser
    env['SERVER_PROTOCOL'] = 'HTTP/1.1'
    # Copy some of the other parameters that are required.
    for name in _REQUIRED_ENVARS:
        env[name] = environ[name]
    # Copy any optional parameters that help everything work properly.
    for name in _OPTIONAL_ENVARS:
        if name in environ:
            env[name] = environ[name]
    # Copy any LC_* variables so sys.getfilesystemencoding() gives the right value.
    for key, val in environ.items():
        if key.startswith('LC_'):
            env[key] = val
    cmd_list = ['git', 'http-backend']
    LOG.debug('_call_git() invoking %s with environment %s', cmd_list, env)
    fork_it = 'push_payload' in environ
    if fork_it:
        LOG.debug2('_call_git() will fork push processing for %s', ctx.config.repo_name)
        env[p4gf_const.P4GF_FORK_PUSH] = ctx.repo_lock.group_id
    caller = environ['proc.caller']
    ec = caller(cmd_list, stdin=input_name, env=env)
    LOG.debug("_call_git() %s returned %s", cmd_list, ec)
    return ec


def send_error_response(sink, start_response, code, body):
    """Send the response headers and return the body.

    Also writes the contents of the OutputSink to the log.

    :param sink: OutputSink from which to read redirected output.
    :param start_response: function for sending status and headers to client.
    :param code: HTTP response code (e.g. 200).
    :param body: response body to be sent to client.

    :return: result of calling start_response().

    """
    LOG.error('redirected output from Git Fusion: %s', sink.readall())
    return p4gf_http_common.send_error_response(start_response, code, body)


@with_timer('WSGI app')
def _wsgi_app(environ, start_response):
    """WSGI application to process the incoming Git client request.

    This is nearly equivalent to p4gf_auth_server.main() with the exception of
    input validation and error handling.
    """
    # pylint: disable=too-many-branches
    p4gf_version_3.log_version_extended()
    p4gf_util.log_environ(LOG, environ, "WSGI")
    p4gf_version_3.version_check()
    LOG.info("Processing HTTP request, pid=%s", os.getpid())
    result = p4gf_http_common.check_file_encoding(start_response)
    if result:
        return result
    try:
        input_file = p4gf_http_common.read_request_data(environ)

        with OutputSink() as sink:
            _response = functools.partial(send_error_response, sink, start_response)
            # pylint 1.2.0 complains about http.server due to assignment in GitFusionRequestHandler
            # pylint 1.3.0 handles this fine with just the disable in GitFusionRequestHandler
            # pylint:disable=attribute-defined-outside-init
            server = GitHttpServer(environ, input_file)
            try:
                server.process()
            except p4gf_http_common.HttpException as e:
                return _response(e.code, e.msg)
            except p4gf_server_common.BadRequestException as e:
                return _response(http.client.BAD_REQUEST, e)
            except p4gf_server_common.PerforceConnectionFailed:
                return _response(http.client.INTERNAL_SERVER_ERROR,
                                 _("Perforce connection failed\n"))
            except p4gf_server_common.SpecialCommandException:
                user_agent = environ.get('HTTP_USER_AGENT')
                if user_agent and not user_agent.startswith('git'):
                    return _response(http.client.OK, sink.readall())
                else:
                    return _response(http.client.NOT_FOUND, sink.readall())
            except p4gf_server_common.CommandError as ce:
                return _response(http.client.FORBIDDEN, ce)
            except p4gf_server_common.RepoNotFoundException as e:
                return _response(http.client.NOT_FOUND, e)
            except p4gf_server_common.RepoInitFailedException:
                return _response(http.client.INTERNAL_SERVER_ERROR,
                                 _("Repository initialization failed\n"))
            except p4gf_server_common.MissingSubmoduleImportUrlException:
                msg = _('Stream imports require a valid http-url be configured.'
                        ' Contact your administrator.')
                return _response(http.client.INTERNAL_SERVER_ERROR, msg)
            except p4gf_atomic_lock.LockConflict as lc:
                return _response(http.client.INTERNAL_SERVER_ERROR, lc)
            except p4gf_server_common.ReadOnlyInstanceException as roie:
                return _response(http.client.FORBIDDEN, roie)
            except Exception as e:  # pylint: disable=broad-except
                return _response(http.client.INTERNAL_SERVER_ERROR, e)

            return []
    finally:
        p4gf_http_common.rm_file_quietly(input_file)


def _handle_cgi():
    """Respond to the incoming CGI request.

    Wrap it in something akin to a WSGI environment, but with lighter
    requirements when it comes to how and when data is written to the client.
    """
    # pylint:disable=too-many-branches

    #
    # In a web server, such as Apache, stdout is redirected to the client,
    # while stderr is written to the server logs, and stdin is supplied by
    # the client.
    #
    headers_set = []
    headers_sent = []

    def wsgi_to_bytes(s):
        """Convert a string to bytes using iso-8859-1 encoding."""
        return s.encode('iso-8859-1')

    def write(data):
        """Ensure headers are written to the client before data."""
        out = sys.stdout.buffer
        if headers_set and not headers_sent:
            # Before the first output, send the stored headers
            headers_sent[:] = headers_set
            status, response_headers = headers_set  # pylint:disable=unbalanced-tuple-unpacking
            out.write(wsgi_to_bytes(NTR('Status: %s\r\n') % status))
            for header in response_headers:
                out.write(wsgi_to_bytes('%s: %s\r\n' % header))
            out.write(wsgi_to_bytes('\r\n'))
        if isinstance(data, str):
            data = wsgi_to_bytes(data)
        out.write(data)
        out.flush()

    def start_response(status, response_headers, exc_info=None):
        """Set the status and headers that will be sent to the client."""
        if exc_info:
            try:
                if headers_sent:
                    # Re-raise original exception if headers sent
                    raise exc_info[1].with_traceback(exc_info[2])
            finally:
                # avoid dangling circular ref
                exc_info = None
        elif headers_set:
            raise AssertionError(_('Headers already set!'))
        headers_set[:] = [status, response_headers]
        # Note: error checking on the headers should happen here, *after*
        # the headers are set. That way, if an error occurs, start_response
        # can only be re-called with exc_info set.
        return write

    # Set up a WSGI-like environment for our WSGI application.
    LOG.debug('_handle_cgi() HTTP request handling started')
    environ = wsgiref.handlers.read_environ()
    environ['wsgi.version'] = (1, 0)
    environ['wsgi.multithread'] = False
    environ['wsgi.multiprocess'] = True
    environ['wsgi.run_once'] = True
    if environ.get('HTTPS', NTR('off')) in (NTR('on'), '1'):
        environ['wsgi.url_scheme'] = 'https'
    else:
        environ['wsgi.url_scheme'] = 'http'
    # Set up the output streams for Git to write to, using the unbuffered
    # binary stream instead of the text wrapper typically supplied. We
    # duplicate the standard output stream because the application will be
    # redirecting sys.stdout to avoid clobbering Git's own output.
    stdout_fobj = open(os.dup(1), 'wb')
    environ['wsgi.output'] = stdout_fobj
    environ['wsgi.errors'] = sys.stderr
    environ['wsgi.input'] = sys.stdin.buffer

    def proc_caller(*args, **kwargs):
        """Delegate to p4gf_proc.call() without much fanfare."""
        return p4gf_proc.call(*args, **kwargs)
    environ['proc.caller'] = proc_caller

    # lighttpd is not entirely compliant with RFC 3875 in that
    # QUERY_STRING is not set to the empty string as required
    # (http://redmine.lighttpd.net/issues/1339).
    if 'QUERY_STRING' not in environ:
        environ['QUERY_STRING'] = ''
    result = None
    try:
        # Invoke our WSGI application within the context of CGI.
        result = _wsgi_app(environ, start_response)
        for data in result:
            # don't send headers until body appears
            if data:
                write(data)
        if not headers_sent:
            # send headers now if body was empty
            write('')
    except Exception:   # pylint:disable=broad-except
        LOG.exception('HTTP processing failed')
        result = p4gf_http_common.send_error_response(
            start_response, http.client.INTERNAL_SERVER_ERROR,
            _('Error, see the Git Fusion log'))
    finally:
        stdout_fobj.close()
    LOG.debug('_handle_cgi() HTTP request handling finished')


def _app_wrapper(environ, start_response):
    """WSGI wrapper to our WSGI/CGI hybrid application.

    :param dict environ: WSGI environment
    :param callable start_response: function to initiate response

    """
    # Direct the request to the LFS server, if appropriate. We do this only
    # for the testing environment, not for production (CGI environments).
    if 'PATH_INFO' in environ and '/info/lfs/' in environ['PATH_INFO']:
        return p4gf_lfs_http_server.app_wrapper(environ, start_response)

    p4gf_http_common.wsgi_begin_request(environ)

    def proc_caller(*args, **kwargs):
        """Call an external process (typically a git command).

        Hack the arguments to subprocess.call() so the output of Git will
        be directed to our socket rather than the console. Need to use
        subprocess directly so the file descriptor will be inherited by the
        child.
        """
        stdout = environ['wsgi.output']
        fobj = None
        if 'stdin' in kwargs:
            fobj = open(kwargs['stdin'], 'rb')
            kwargs['stdin'] = fobj
        kwargs['close_fds'] = False
        kwargs['stdout'] = stdout
        try:
            # Git client is not happy without the status line. Since we are
            # the origin server, output the first few lines that are
            # expected in a standard HTTP response.
            handler = environ['wsgi.handler']
            handler.send_response(200, 'OK')
            handler.flush_headers()
            return subprocess.call(*args, **kwargs)
        finally:
            if fobj:
                fobj.close()
    environ['proc.caller'] = proc_caller

    handler = functools.partial(_wsgi_app, environ, start_response)
    return p4gf_http_common.wsgi_run_request_handler(handler, start_response)


class GitFusionHandler(wsgiref.handlers.SimpleHandler):

    """Handler subclass.

    A WSGI handler that allows for sending data that already includes
    relevant headers, without the need for calling start_response.
    """

    # pylint: disable=too-many-public-methods

    def setup_environ(self):
        """Insert additional properties into the environment."""
        wsgiref.handlers.BaseHandler.setup_environ(self)
        # Provide the means for our customized WSGI application to write
        # back directly to the client, circumventing the WSGI Python code.
        self.environ['wsgi.output'] = self.stdout
        self.environ['wsgi.handler'] = self.request_handler

    def finish_content(self):
        """Only finish the content if the headers have been set."""
        if self.headers:
            wsgiref.handlers.SimpleHandler.finish_content(self)

    def write(self, data):
        """Write whatever is given.

        Send status and headers if they have been provided, otherwise assume
        they have been provided and pass through to _write().
        """
        if self.status and not self.headers_sent:
            # Before the first output, send the stored headers
            self.send_headers()
        self.headers_sent = True
        self._write(data)
        self._flush()


class GitFusionRequestHandler(wsgiref.simple_server.WSGIRequestHandler):

    """Subclass Handler to make writing headers optional.

    In order to make writing headers optional, need to override this class
    and set up our own handler, defined above.
    """

    # pylint: disable=too-many-public-methods

    def handle(self):
        """Prepare to handle an incoming request."""
        # pylint:disable=attribute-defined-outside-init

        # Identical to WSGIRequestHandler.handle() except for the
        # construction of the handler.
        self.raw_requestline = self.rfile.readline()
        if not self.parse_request():
            # An error code has been sent, just exit
            return

        handler = GitFusionHandler(
            self.rfile, self.wfile, self.get_stderr(), self.get_environ()
        )
        handler.request_handler = self
        handler.run(self.server.get_app())


@with_timer('HTTP main')
def main():
    """Parse command line arguments and decide what should be done."""
    desc = _("""p4gf_http_server.py handles http(s) requests. Typically it
is run via a web server and protected by some form of user
authentication. The environment variable REMOTE_USER must be set to
the name of a valid Perforce user, which is taken to be the user
performing a pull or push operation.
""")
    epilog = _("""If the --port argument is given then a simple HTTP server
will be started, listening on the specified port. In lieu of REMOTE_USER, the
user name is extracted from the URI, which starts with "/~", followed by the
user name. To stop the server, send a terminating signal to the process.
""")
    log_l10n()
    parser = p4gf_util.create_arg_parser(desc, epilog=epilog)
    parser.add_argument('-p', '--port', type=int,
                        help=_('port on which to listen for HTTP requests'))
    args = parser.parse_args()
    if args.port:
        LOG.info("Listening for HTTP requests on port %s, pid=%s", args.port, os.getpid())
        httpd = wsgiref.simple_server.make_server(
            '', args.port, _app_wrapper, handler_class=GitFusionRequestHandler)
        print(_('Serving on port {}...').format(args.port))
        p4gf_http_common.wsgi_install_signal_handler(httpd)
        p4gf_proc.install_stack_dumper()
        httpd.serve_forever()
    else:
        # Assume we are running inside a web server...
        p4gf_proc.install_stack_dumper()
        _handle_cgi()


if __name__ == "__main__":
    # Get the logging configured properly...
    with p4gf_log.ExceptionLogger(squelch=False, write_to_stderr_=True):
        try:
            main()
        except Exception:   # pylint:disable=broad-except
            LOG.exception('HTTP main failed')
