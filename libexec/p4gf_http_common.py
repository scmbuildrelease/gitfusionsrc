#! /usr/bin/env python3.3
"""Common HTTP handling code."""

import functools
import http.client
import logging
import os
import shutil
import signal
import sys
import wsgiref.util

import p4gf_const
import p4gf_env_config
from p4gf_l10n import _, NTR
import p4gf_log
import p4gf_server_common
import p4gf_tempfile

LOG = logging.getLogger(__name__)

# Path info suffixes from typical Git HTTP requests, including LFS.
GIT_SUFFIXES = [
    'info/refs',
    'info/lfs',
    'HEAD',
    p4gf_server_common.CMD_GIT_UPLOAD_PACK,
    p4gf_server_common.CMD_GIT_RECEIVE_PACK,
    p4gf_server_common.CMD_LFS_OBJECTS
]

# Parameters that are required for HTTP request handling.
REQUIRED_HTTP_PARAMS = [
    ('PATH',           http.client.INTERNAL_SERVER_ERROR, _('Missing PATH value')),
    ('PATH_INFO',      http.client.BAD_REQUEST,           _('Missing PATH_INFO value')),
    ('QUERY_STRING',   http.client.BAD_REQUEST,           _('Missing QUERY_STRING value')),
    ('REQUEST_METHOD', http.client.BAD_REQUEST,           _('Missing REQUEST_METHOD value')),
    ('REMOTE_USER',    http.client.UNAUTHORIZED,          _('Missing REMOTE_USER value'))
]

CHUNK_SIZE = 65536
TE_HEADER = 'HTTP_TRANSFER_ENCODING'


class HttpException(p4gf_server_common.ServerCommonException):

    """Bad arguments or similar errors."""

    def __init__(self, code, msg):
        """Initialize the thing."""
        super(HttpException, self).__init__()
        self.code = code
        self.msg = msg

    def __str__(self):
        """Return the string representation of this exception."""
        return "HTTP {0}, {1}".format(self.code, self.msg)


class HttpServer(p4gf_server_common.Server):

    """Base class for HTTP servers."""

    def __init__(self, environ):
        """Initialize an instance of HttpServer."""
        super(HttpServer, self).__init__()
        self._environ = environ
        # Some code needs a hint that the request is coming over one
        # protocol (HTTP) or the other (SSH).
        os.environ['REMOTE_ADDR'] = environ['REMOTE_ADDR']

    def record_access(self):
        """Record the access of the repository in the audit log."""
        args = {
            'userName': self.user,
            'command': self.command,
            'repo': self.repo_name
        }
        p4gf_log.record_http(args, self._environ)

    def record_error(self, msg):
        """Record the given error message to the audit log."""
        args = {
            'userName': self.user,
            'command': self.command + NTR('-failed'),
            'repo': self.repo_name
        }
        p4gf_log.record_error(msg, args, self._environ)

    @property
    def environ(self):
        """Accessor for the environment dictionary."""
        return self._environ

    def check_required_params(self):
        """Sanity check the request, raising an exception if invalid."""
        for (name, status, msg) in REQUIRED_HTTP_PARAMS:
            if name not in self._environ:
                raise HttpException(status, msg)

    def get_user(self):
        """Retrieve the username and store it in the environment."""
        self.user = self._environ['REMOTE_USER']
        # Some places in the Perforce-to-Git phase will need to know the
        # name of client user, so set that here. As for Git-to-Perforce,
        # that is handled later by setting the REMOTE_USER envar. Notice
        # also that we're setting os.environ and not 'environ'.
        os.environ[p4gf_const.P4GF_AUTH_P4USER] = self.user

    def get_repo_name_git(self):
        """Extract repository name by removing the expected request suffixes."""
        LOG.debug('get_repo_name_git() PATH_INFO: %s', self._environ['PATH_INFO'])
        spi = SplitPathInfo.from_path_info(self._environ['PATH_INFO'])
        if not spi.repo_name_git:
            raise p4gf_server_common.BadRequestException(
                _("Missing required repository name in URL\n"))
        self.repo_name_git = spi.repo_name_git
        if spi.suffix:
            self._environ['PATH_INFO'] = "/" + spi.suffix
        LOG.debug("new PATH_INFO %s repo_name_git %s",
                  self._environ['PATH_INFO'], self.repo_name_git)

    def _write_motd(self):
        """Skip message of the day with HTTP."""
        pass


class SplitPathInfo(object):

    """Extracts the repo_name_git and suffix from a PATH_INFO value.

    PATH_INFO     = "/dir/p4gf_repo.git/info/refs"
    repo_name_git =  "dir/p4gf_repo.git"
    suffix        =                    "info/refs"

    """

    def __init__(self):
        """Initialize an instance of SplitPathInfo."""
        self.orig = None
        self.repo_name_git = None
        self.suffix = None
        self._command = None

    @staticmethod
    def from_path_info(path_info):
        """Parse various URI constructs to constituent parts."""
        path_end = len(path_info)
        for suffix in GIT_SUFFIXES:
            try:
                path_end = path_info.index("/" + suffix)
                break
            except ValueError:
                pass
        # slice away the leading slash and the trailing git request suffixes
        se1f = SplitPathInfo()
        se1f.orig = path_info
        se1f.repo_name_git = path_info[1:path_end]
        se1f.suffix = path_info[1+path_end:]
        if se1f.suffix.startswith("info/lfs/"):
            # Avoid having to deal with the useless prefix everywhere.
            se1f.suffix = se1f.suffix[9:]
        return se1f

    @property
    def command(self):
        """Extract the command from the path info suffix.

        :return: typically "objects" for LFS requests.

        """
        if self._command is None:
            if '/' in self.suffix:
                self._command = self.suffix[:self.suffix.find('/')]
            else:
                self._command = self.suffix
        return self._command


def is_cgi():
    """Return True if operating in CGI mode, False otherwise."""
    # In a real CGI environment, the os.environ is the canonical source of
    # truth regarding our operating environment.
    return 'GATEWAY_INTERFACE' in os.environ and 'CGI' in os.environ['GATEWAY_INTERFACE']


def send_error_response(start_response, code, body):
    """Send the response headers and return the body.

    :param start_response: function for sending status and headers to client.
    :param code: HTTP response code (e.g. 200).
    :param body: response body to be sent to client.

    """
    # Log the error so the admin can find it, since typically the error
    # is not displayed by the Git client when using HTTP.
    LOG.error(body)
    # make sure it's UTF-8 bytes
    if type(body) is not bytes:
        body = str(body).encode('UTF-8')
    # Keep the content type to exactly 'text/plain' so that Git
    # will show our error messages.
    headers = [('Content-Type', 'text/plain;charset=UTF-8'),
               ('Content-Length', str(len(body)))]
    # format the response code with the W3C name
    status = "{} {}".format(code, http.client.responses[code])
    start_response(status, headers)
    return [body]


def check_file_encoding(start_response):
    """Ensure the file system encoding is set correctly.

    :param start_response: function for sending status and headers to client.
    :return: None if okay, response body if invalid.

    """
    encoding = sys.getfilesystemencoding()
    if encoding == 'ascii':
        # This encoding is wrong and will eventually lead to problems.
        LOG.error("Using 'ascii' file encoding will ultimately result in errors, "
                  "please set LANG/LC_ALL to 'utf-8' in web server configuration.")
        return send_error_response(start_response, http.client.INTERNAL_SERVER_ERROR,
                                   _("Filesystem encoding not set to acceptable value.\n"))
    return None


def wsgi_install_signal_handler(httpd):
    """Install a signal handler for terminiating signals.

    The signal handler will shut down the HTTP server and exit, in addition
    to logging the fact that a particular signal was received.

    :param httpd: instance of HTTP server to be shut down on a signal.

    """
    def _signal_handler(signum, _frame):
        """Ensure the web server is shutdown properly."""
        LOG.info("Received signal %s, pid=%s, shutting down", signum, os.getpid())
        httpd.server_close()
        sys.exit(0)
    LOG.debug("installing HTTP server signal handler, pid=%s", os.getpid())
    signal.signal(signal.SIGHUP, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGQUIT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGTSTP, _signal_handler)


def wsgi_begin_request(environ):
    """Set up request environment for WSGI servers.

    Should be called at the beginning of each WSGI request.

    :param dict environ: request environment.

    """
    p4gf_env_config.Env_config.reset_gf_environment()
    wsgiref.util.setup_testing_defaults(environ)
    log_var = p4gf_const.P4GF_TEST_LOG_CONFIG_PATH
    if log_var not in environ and log_var in os.environ:
        environ[log_var] = os.environ[log_var]
    # If the URL begins with /~<user>/ then we extract that and use it as
    # the name of the authenticated user, which is useful for our automated
    # tests, in which there is no real request authorization in place.
    path_info = environ['PATH_INFO']
    # (save the original value)
    environ['ORIG_PATH_INFO'] = path_info
    if path_info.startswith('/~'):
        slash_idx = path_info.index('/', 2)
        environ['REMOTE_USER'] = path_info[2:slash_idx]
        environ['PATH_INFO'] = path_info[slash_idx:]


def wsgi_run_request_handler(handler, start_response):
    """Run the given request handling function.

    :param handler: function to handle the request, returns the WSGI result.
    :param start_response: function for sending error messages, if needed.

    :return: whatever the handler function returns, unless an error occurs,
             in which case the result will be the error message.

    """
    # This is typically a long running process, make sure to return to the
    # current working directory.
    cwd = os.getcwd()
    try:
        result = handler()
    except Exception:   # pylint: disable=broad-except
        LOG.exception('WSGI HTTP processing failed')
        result = send_error_response(
            start_response, http.client.INTERNAL_SERVER_ERROR, _('See the Git Fusion log'))
    finally:
        # Return to the previous working directory lest the T4 test rip the
        # current one (typically the Git working tree) out from under us.
        os.chdir(cwd)
    return result


def read_request_data(environ):
    """Read the incoming request data to a temporary file.

    Handles both WSGI and CGI environments.

    :param dict environ: WSGI request environment.

    :return: name of temporary file containing request data.

    """
    # Read the input from the client.
    incoming = environ['wsgi.input']
    stdin_fobj = p4gf_tempfile.new_temp_file(prefix='http-client-input-', delete=False)
    LOG.debug('read_request_data() writing stdin to %s', stdin_fobj.name)
    if is_cgi():
        # Running in CGI mode as a WSGI application. In a hosted CGI
        # environment, the matter of content-length and transfer-encoding
        # is handled for us by the server. We simply read the input until
        # the EOF is encountered.
        shutil.copyfileobj(incoming, stdin_fobj)
    else:
        # Running within the WSGI simple server.
        # For more information on the idiosyncrasies within WSGI 1.0, see
        # http://blog.dscpl.com.au/2009/10/details-on-wsgi-10-amendmentsclarificat.html
        try:
            content_length = int(environ.get('CONTENT_LENGTH', 0))
        except ValueError:
            content_length = 0
        method = environ['REQUEST_METHOD']
        if TE_HEADER in environ and environ[TE_HEADER] == 'chunked':
            reader = ChunkedTransferReader(stdin_fobj)
            reader.read(incoming)
        elif content_length and (method == "POST" or method == "PUT"):
            # To avoid blocking forever reading input from the client, must
            # read _only_ the number of bytes specified in the request
            # (which happens to permit HTTP/1.1 keep-alive connections).
            while content_length > 0:
                length = min(65536, content_length)
                buf = incoming.read(length)
                if not buf:
                    break
                stdin_fobj.write(buf)
                content_length -= len(buf)
    stdin_fobj.close()
    return stdin_fobj.name


def rm_file_quietly(fpath):
    """Remove a (temporary) file without raising any exception."""
    if fpath and os.path.exists(fpath):
        try:
            os.unlink(fpath)
        except OSError:
            LOG.warning("unable to delete file %s", fpath)


class ChunkedTransferReader():

    """Read and translate chunked data from one stream to another."""

    def __init__(self, outgoing):
        """Initialize an instance of ChunkedTransferReader."""
        self.keep_reading = True
        self.outgoing = outgoing

    def _received_data(self, data):
        """Received data."""
        self.outgoing.write(data)

    def _finished_data(self, data):
        """Received the last data."""
        self.keep_reading = False
        self.outgoing.write(data)

    def read(self, incoming):
        """Read from the incoming stream and write the decoded results."""
        got_data = functools.partial(self._received_data)
        all_done = functools.partial(self._finished_data)
        decoder = ChunkedTransferDecoder(got_data, all_done)
        while self.keep_reading:
            data = incoming.read1(65536)
            if data is not None:
                decoder.read_data(data)
            elif self.keep_reading:
                raise RuntimeError(_("end of stream reached"))


class ChunkedTransferDecoder():

    """Decode a "chunked" Transfer-Encoding stream.

    Basic algorithm borrowed from the Twisted framework.

    """

    def __init__(self, data_func, finish_func):
        """Initialized an instance of ChunkedTransferDecoder."""
        self._bytes_to_read = 0
        self._data_func = data_func
        self._finish_func = finish_func
        self._buffer = b''
        self._state = self._chunk_length

    def _chunk_length(self, data):
        """Process the chunk length value."""
        if b'\r\n' in data:
            line, rest = data.split(b'\r\n', 1)
            parts = line.split(b';')
            try:
                self._bytes_to_read = int(parts[0], 16)
            except ValueError:
                raise RuntimeError(_("Chunk-size must be an integer."))
            if self._bytes_to_read == 0:
                self._state = self._trailer
            else:
                self._state = self._body
            return rest
        else:
            self._buffer = data
            return b''

    def _line_end(self, data):
        """Prepare to read the chunk."""
        if data.startswith(b'\r\n'):
            self._state = self._chunk_length
            return data[2:]
        else:
            self._buffer = data
            return b''

    def _trailer(self, data):
        """Invoke the finished function."""
        if data.startswith(b'\r\n'):
            data = data[2:]
            self._state = self._finished
            self._finish_func(data)
        else:
            self._buffer = data
        return b''

    def _body(self, data):
        """Process the received chunk data."""
        if len(data) >= self._bytes_to_read:
            chunk, data = data[:self._bytes_to_read], data[self._bytes_to_read:]
            self._data_func(chunk)
            self._state = self._line_end
            return data
        elif len(data) < self._bytes_to_read:
            self._bytes_to_read -= len(data)
            self._data_func(data)
            return b''

    def _finished(self, _data):
        """Should never be called under normal circumstances."""
        # pylint: disable=no-self-use
        raise RuntimeError(_("data received after last chunk!"))

    def read_data(self, data):
        """Decode the chunked data."""
        data = self._buffer + data
        self._buffer = b''
        while data:
            data = self._state(data)
