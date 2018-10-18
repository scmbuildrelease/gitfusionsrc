#! /usr/bin/env python3.3
"""Large File Storage over HTTP request handling."""

import functools
import hashlib
import http.client
import json
import logging
import os
import re
import shutil
import socket
import sys
import wsgiref.handlers
import wsgiref.simple_server
import wsgiref.util

# Ensure the system path includes our modules.
try:
    import p4gf_version_3
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import p4gf_version_3

import p4gf_config
import p4gf_const
import p4gf_env_config  # pylint: disable=unused-import
import p4gf_http_common
from p4gf_l10n import _, log_l10n
from p4gf_lfs_file_spec import LFSFileSpec
import p4gf_log
import p4gf_proc
from p4gf_profiler import with_timer
import p4gf_server_common
import p4gf_util

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_lfs_http_server")

_CONTENT_TYPE_LFS = 'application/vnd.git-lfs'
_CONTENT_TYPE_LFS_JSON = 'application/vnd.git-lfs+json'
_CONTENT_TYPE_LFS_JSON_UT8 = 'application/vnd.git-lfs+json; charset=utf-8'
_CONTENT_TYPE_OCTET_STREAM = 'application/octet-stream'
BATCH_PATH_INFO = '/objects/batch'
VALID_SHA256_RE = re.compile("^[a-fA-F0-9]{64}$")
HTTP_URL_REGEX = re.compile(r'http[s]?://([^/])+/([^/]+)')


class LargeFileHttpServer(p4gf_http_common.HttpServer):

    """HttpServer subclass for LFS over HTTP."""

    def __init__(self, environ, start_response, input_file):
        """Initialize the LFS over HTTP server.

        :param dict environ: WSGI environment
        :param callable start_response: function to initiate response
        :param str input_file: path to file containing request data.

        """
        super(LargeFileHttpServer, self).__init__(environ)
        self._start_response = start_response
        self._input_file = input_file

    def _load_request_data(self):
        """Read the request data (from the temporary file).

        :raises UnicodeDecodeError: if the file is not decipherable as UTF-8.

        """
        with open(self._input_file, 'r', encoding='utf-8') as f:
            data = f.read()
        return data

    def _init_repo(self, _ctx):
        """LFS does not initialize the repository."""
        return False

    def _init_system(self):
        """LFS does not initialize the Git Fusion system."""
        pass

    def before(self):
        """setup user, command, etc."""
        self.check_required_params()
        self.get_repo_name_git()
        self.get_user()
        self._get_command()
        # Skip the preliminary permissions checks so that we can determine
        # if the request is a read or write operation, then apply the
        # checks at the appropriate time (especially for batch).
        self.skip_perms_check = True

    def check_lfs_enabled(self):
        """Validate repo configuration if processing an LFS request.

        If we're processing a Git LFS request, but the current repo is not
        configured to allow Git LFS requests, reject.

        """
        method = self.environ.get("REQUEST_METHOD")
        path_info = self.environ['PATH_INFO']
        if method == 'POST' and path_info == BATCH_PATH_INFO:
            # Special case batch requests as a pass-through at this point,
            # to be responded to appropriately later in the process. The
            # client is not expecting a 400 for batch requests.
            return

        # Ensure the repository has already been initialized as
        # initialization via LFS request is forbidden.
        try:
            p4gf_config.RepoConfig.from_depot_file(
                self.repo_name, self.p4, create_if_missing=False)
        except p4gf_config.ConfigLoadError:
            raise p4gf_server_common.BadRequestException(_("Repo not yet initialized\n"))

        enabled = self.repo_config.getboolean(p4gf_config.SECTION_PERFORCE_TO_GIT,
                                              p4gf_config.KEY_GIT_LFS_ENABLE)
        if not enabled:
            raise p4gf_server_common.BadRequestException(
                _("Git LFS not enabled for this repo.\n"))

        http_url = self._get_lfs_url()
        LOG.debug("check_lfs_enabled() HTTP URL=%s", http_url)

    def check_special_command(self):
        """Cleverly do nothing for LFS."""
        pass

    def _get_command(self):
        """Retrieve the Git command from the request environment.

        Determine if this is a pull or push operation, returning the
        canonical git command (i.e. git-upload-pack or git-receive-pack).
        Returns None if the command does not match what is expected, or is
        not provided.

        """
        if not self.command:
            # get_repo_name_git() already stripped repo name from
            # PATH_INFO, but that's okay, SplitPathInfo() can survive
            # that side effect and still find the suffix/command.
            spi = p4gf_http_common.SplitPathInfo.from_path_info(self.environ['PATH_INFO'])
            cmd = spi.command
            LOG.debug('_get_command() retrieved %s command', cmd)
            # Check for Git LFS command. Anything else is wrong.
            if cmd == p4gf_server_common.CMD_LFS_OBJECTS:
                self.command = cmd

    def _get_lfs_url(self, validate=True):
        """Retrieve the appropriate URL for the LFS responses."""
        if 'P4GF_LFS_URL' in self.environ:
            # For certain tests, we get the URL via the environment.
            http_url = self.environ['P4GF_LFS_URL']
        else:
            http_url = _format_http_url(self.repo_config)
            if validate and http_url is None:
                raise p4gf_server_common.BadRequestException(
                    _("Git LFS enabled, but HTTP URL not set for this repo.\n"))
        return http_url

    def start_response(self, status_code, response_headers, exc_info=None):
        """Begin the response to the client.

        :param int status_code: HTTP status code (e.g. 200).
        :param list response_headers: list of 2-tuples for response headers.
        :param exc_info: exception information, if any.

        """
        reason = http.client.responses[status_code]
        status = "{} {}".format(status_code, reason)
        return self._start_response(status, response_headers, exc_info)

    def process_request(self, ctx, _repo_created):
        """Respond to the Git LFS request with metadata or content.

        Git LFS wants to exchange metadata (if json) or actual large
        file content (if not json) for a single large file.
        """
        # pylint:disable=too-many-branches
        # before() disabled checks which we must now enable once again
        self.skip_perms_check = False
        _detect_client_and_version(self.environ.get("HTTP_USER_AGENT"))
        # Route to the appropriate function based on the nature of the
        # request (i.e. GET, PUT, or POST, metadata or content).
        method = self.environ.get("REQUEST_METHOD")
        path_info = self.environ.get('PATH_INFO')
        content_type = self.environ.get("CONTENT_TYPE")
        LOG.info("received LFS request (%s): %s %s", self.command, method, path_info)
        if method == 'POST':
            if path_info == BATCH_PATH_INFO:
                if content_type == _CONTENT_TYPE_LFS_JSON or \
                        content_type == _CONTENT_TYPE_LFS_JSON_UT8:
                    self._process_lfs_batch(ctx)
                else:
                    self._wrong_content_type(content_type)
            else:
                if content_type == _CONTENT_TYPE_LFS_JSON_UT8:
                    self._process_lfs_put_metadata(ctx)
                else:
                    self._wrong_content_type(content_type)
        elif method == 'PUT':
            if content_type == _CONTENT_TYPE_OCTET_STREAM:
                self._process_lfs_put_content(ctx)
            else:
                self._wrong_content_type(content_type)
        elif method == 'GET':
            accept = self.environ.get("HTTP_ACCEPT")
            if accept == _CONTENT_TYPE_LFS_JSON_UT8:
                self._process_lfs_get_metadata(ctx)
            else:
                # With git-lfs 1.0.1 the GET request for large files does
                # not include an Accept header.
                self._process_lfs_get_content(ctx)
        else:
            raise p4gf_server_common.BadRequestException(
                "unsupported request method: {0}".format(method))

    def _process_lfs_put_metadata(self, ctx):
        """Process the PUT metadata request from an LFS client."""
        self._ensure_not_readonly(ctx)
        # Load the HTTP POST payload. It's a JSON text block.
        try:
            req_str = self._load_request_data()
        except UnicodeDecodeError:
            msg = _('request body must be encoded as UTF-8')
            write = self.start_response(400, [])
            write(msg.encode('utf-8'))
            return

        request = _guard_incoming_json(req_str)
        _log_payload("LFS PUT metadata request: %s", req_str)
        http_url = self._get_lfs_url()
        oid = request["oid"]

        # Check if object already exists in our cache or the depot.
        lfs_spec = LFSFileSpec(oid=oid)
        if lfs_spec.exists_in_cache(ctx) or lfs_spec.exists_in_depot(ctx):
            write = self.start_response(200, [])
            write(''.encode('utf-8'))
            return

        href = _construct_lfs_href(self.environ, http_url, oid)
        response = {
            "oid": oid,
            "size": request["size"],
            "_links": {
                "upload": {
                    "href": href,
                    "header": {
                        "Accept": _CONTENT_TYPE_LFS,
                        "Authorization": self.environ.get("HTTP_AUTHORIZATION")
                    }
                }
            }
        }
        body = json.dumps(response)
        _log_payload("LFS PUT metadata response: %s", body)
        headers = [
            ('Content-Length', str(len(body))),
            ('Content-Type', _CONTENT_TYPE_LFS_JSON)
        ]
        write = self.start_response(202, headers)
        write(body.encode('utf-8'))

    def _process_lfs_put_content(self, ctx):
        """Process the PUT content request from an LFS client."""
        self._ensure_not_readonly(ctx)
        oid = os.path.basename(self.environ['PATH_INFO'])
        LOG.debug("LFS put content request: %s", oid)
        # Read the file in chunks and compute the sha256 to verify it matches.
        self._validate_file_checksum(oid)
        lfs_spec = LFSFileSpec(oid=oid)
        if not lfs_spec.exists_in_cache(ctx):
            fname = lfs_spec.cache_path(ctx)
            os.makedirs(os.path.dirname(fname))
            fd = None
            try:
                # Open the file for exclusive create access, which will
                # raise an exception if the file already exists. This
                # prevents data races among concurrent processes.
                fd = os.open(fname, os.O_CREAT | os.O_EXCL)
            except OSError as err:
                LOG.warning('attempt to push identical object %s: %s', oid, err)
            finally:
                if fd:
                    # Simply rename/move the file we saved earlier.
                    shutil.move(self._input_file, fname)
                    os.close(fd)
        write = self.start_response(200, [])
        write(''.encode('utf-8'))
        LOG.debug("LFS put content complete: %s", oid)

    def _process_lfs_get_metadata(self, ctx):
        """Process the GET metadata request from an LFS client."""
        self._check_perms()
        oid = os.path.basename(self.environ['PATH_INFO'])
        LOG.debug("LFS GET metadata request: %s", oid)
        lfs_spec = LFSFileSpec(oid=oid)
        if not lfs_spec.exists_in_cache(ctx):
            if not lfs_spec.exists_in_depot(ctx):
                LOG.debug('LFS GET content missing for %s', oid)
                write = self.start_response(404, [])
                write(''.encode('utf-8'))
                return
            file_size = _get_object_size(ctx, lfs_spec)
        else:
            fname = lfs_spec.cache_path(ctx)
            file_size = os.stat(fname).st_size
        http_url = self._get_lfs_url()
        href = _construct_lfs_href(self.environ, http_url, oid)
        response = {
            "oid": oid,
            "size": file_size,
            "_links": {
                "download": {
                    "href": href,
                    "header": {
                        "Accept": _CONTENT_TYPE_LFS
                    }
                }
            }
        }
        body = json.dumps(response)
        _log_payload("LFS GET metadata response: %s", body)
        headers = [
            ('Content-Length', str(len(body))),
            ('Content-Type', _CONTENT_TYPE_LFS_JSON)
        ]
        write = self.start_response(200, headers)
        write(body.encode('utf-8'))

    def _process_lfs_get_content(self, ctx):
        """Process the GET content request from an LFS client."""
        self._check_perms()
        oid = os.path.basename(self.environ['PATH_INFO'])
        LOG.debug("LFS GET content request: %s", oid)
        lfs_spec = LFSFileSpec(oid=oid)
        if not lfs_spec.exists_in_cache(ctx):
            if not lfs_spec.exists_in_depot(ctx):
                LOG.debug('LFS GET content missing for %s', oid)
                write = self.start_response(404, [])
                write(''.encode('utf-8'))
                return
            depot_path = lfs_spec.depot_path(ctx)
            cache_path = lfs_spec.cache_path(ctx)
            with p4gf_util.raw_encoding(ctx.p4):
                ctx.p4run('print', '-q', '-o', cache_path, depot_path)
        else:
            cache_path = lfs_spec.cache_path(ctx)
        file_size = os.stat(cache_path).st_size
        headers = [
            ('Content-Type', 'application/octet-stream'),
            ('Content-Length', str(file_size))
        ]
        write = self.start_response(200, headers)
        with open(cache_path, 'rb') as fobj:
            while True:
                buf = fobj.read(131072)
                if not buf:
                    break
                write(buf)
        LOG.debug("LFS GET content complete: %s", oid)

    def _process_lfs_batch(self, ctx):
        """Process the batch API request from an LFS client."""
        # Load the HTTP POST payload. It's a JSON text block.
        try:
            req_str = self._load_request_data()
        except UnicodeDecodeError:
            msg = 'request body must be encoded as UTF-8'
            write = self.start_response(400, [])
            write(msg.encode('utf-8'))
            return

        request = _guard_incoming_json(req_str, batch=True)
        _log_payload("LFS batch request: %s", req_str)

        # Delayed permissions check so we can insert the errors in the JSON
        # response, returning a 200 Ok, as dictated by the batch API.
        try:
            if self._get_lfs_url(validate=False) is None:
                raise p4gf_server_common.CommandError(_('missing http-url config setting'))
            self._ensure_not_readonly(ctx)
            self._check_perms()
            objects = self._build_batch_response(ctx, request)
        except p4gf_server_common.CommandError as ce:
            objects = self._build_error_response(request, str(ce))
        except p4gf_server_common.ReadOnlyInstanceException as roie:
            objects = self._build_error_response(request, str(roie))

        response = {
            "objects": objects
        }
        body = json.dumps(response)
        _log_payload("LFS batch response: %s", body)
        headers = [
            ('Content-Length', str(len(body))),
            ('Content-Type', _CONTENT_TYPE_LFS_JSON)
        ]
        write = self.start_response(200, headers)
        write(body.encode('utf-8'))

    def _build_batch_response(self, ctx, request):
        """Build the response to the batch request."""
        objects = []
        http_url = self._get_lfs_url()
        for obj in request['objects']:
            oid = obj['oid']
            resp = {
                "oid": oid
            }
            href = _construct_lfs_href(self.environ, http_url, oid)
            lfs_spec = LFSFileSpec(oid=oid)
            if not lfs_spec.exists_in_cache(ctx):
                if not lfs_spec.exists_in_depot(ctx):
                    resp['actions'] = {
                        "upload": {
                            "href": href
                        }
                    }
                else:
                    resp['size'] = _get_object_size(ctx, lfs_spec)
            else:
                fname = lfs_spec.cache_path(ctx)
                resp['size'] = os.stat(fname).st_size
            if 'actions' not in resp:
                resp['actions'] = {
                    "download": {
                        "href": href
                    }
                }
            objects.append(resp)
        return objects

    @staticmethod
    def _build_error_response(request, error):
        """Build an error response to the batch request."""
        objects = []
        for obj in request['objects']:
            resp = {
                "oid": obj['oid'],
                "error": {
                    "code": 403,
                    "message": error
                }
            }
            objects.append(resp)
        return objects

    def _validate_file_checksum(self, oid):
        """Ensure the incoming file content has the correct checksum."""
        if not VALID_SHA256_RE.match(oid):
            LOG.debug('oid not a valid SHA256 value: %s', )
            raise p4gf_server_common.BadRequestException("file checksum not a valid SHA256")
        m = hashlib.sha256()
        with open(self._input_file, 'rb') as fobj:
            while True:
                buf = fobj.read(131072)
                if not buf:
                    break
                m.update(buf)
        if oid != m.hexdigest():
            LOG.debug("expected %s but got %s for %s", oid, m.hexdigest(), self._input_file)
            raise p4gf_server_common.BadRequestException("file checksum does not match oid")

    def _wrong_content_type(self, content_type):
        """Report an incorrect content type value."""
        LOG.debug('incorrect content-type: %s', content_type)
        write = self.start_response(406, [])
        write(_('unrecognized Content-Type header').encode('utf-8'))

    def _wrong_accept_header(self, accept):
        """Report an incorrect accept header value."""
        LOG.debug('incorrect http-accept: %s', accept)
        write = self.start_response(406, [])
        write(_('unrecognized Http-Accept header').encode('utf-8'))

    def _ensure_not_readonly(self, ctx):
        """Raise an exception if the instance or repo are read-only.

        Also checks permissions as if performing a push operation.

        """
        self._check_perms()
        if p4gf_const.READ_ONLY:
            LOG.debug('push to a read-only instance prohibited')
            raise p4gf_server_common.ReadOnlyInstanceException(
                _('LFS upload to read-only instance prohibited'))
        read_only_repo = ctx.repo_config.getboolean(
            p4gf_config.SECTION_REPO, p4gf_config.KEY_READ_ONLY, fallback=False)
        if read_only_repo:
            LOG.debug('push to a read-only repository prohibited')
            raise p4gf_server_common.ReadOnlyInstanceException(
                _('LFS upload to read-only repository prohibited'))


def _guard_incoming_json(request_data, batch=False):
    """Validate the incoming LFS request JSON data.

    :param str request_data: JSON formatted LFS request.
    :param bool batch: if True, treat as batch request (default False).
    :return: parsed objects from JSON data.

    """
    try:
        data = json.loads(request_data)
        if batch:
            # validate the batch request data
            if 'operation' not in data:
                LOG.debug('operation missing from JSON')
                raise ValueError()
            oper = data['operation']
            if oper != 'upload' and oper != 'download' and oper != 'verify':
                LOG.debug('operation "%s" not supported', oper)
                raise ValueError()
            if 'objects' not in data:
                LOG.debug('objects missing from JSON')
                raise ValueError()
            for obj in data['objects']:
                _validate_json_object(obj)
        else:
            # validate the oid and size values
            _validate_json_object(data)
        return data
    except ValueError as ve:
        LOG.debug('invalid JSON: %s', ve)
        raise p4gf_server_common.BadRequestException(str(ve))


def _validate_json_object(data):
    """Validate the individual 'object' in the given JSON data structure.

    :raises ValueError: Raised if object is invalid.

    """
    if 'oid' not in data:
        raise ValueError(_('oid missing from JSON'))
    oid = data['oid']
    if not isinstance(oid, str):
        raise ValueError(_('oid not a string type, {oid_type}')
                         .format(oid_type=type(oid)))
    if not VALID_SHA256_RE.match(oid):
        raise ValueError(_('oid not a valid SHA256 value: {oid}').format(oid=oid))

    # validate the size value
    if 'size' not in data:
        raise ValueError(_('size missing from JSON'))
    size = data['size']
    if not isinstance(size, int):
        raise ValueError(_('size not an int type, {size_type}')
                         .format(size_type=type(size)))
    if int(size) < 0:
        # The git-lfs client sometimes sends a size of zero, which differs
        # from earlier releases. Even though it makes no sense, we must
        # allow it.
        raise ValueError(_('size cannot be less than zero, {size}')
                         .format(size=size))


def _get_object_size(ctx, lfs_spec):
    """Determine the size of the object stored in Git Fusion."""
    depot_path = lfs_spec.depot_path(ctx)
    cmd = ['fstat', '-m1', '-Ol', '-T', 'fileSize', depot_path]
    return int(p4gf_util.first_value_for_key(ctx.p4run(cmd), 'fileSize'))


def _construct_lfs_href(environ, base_url, suffix):
    """Construct a URL for the LFS response."""
    # BEWARE OF LEADING SLASHES!
    middle = 'objects'
    if 'ORIG_PATH_INFO' in environ and '/info/lfs/' in environ['ORIG_PATH_INFO']:
        middle = 'info/lfs/objects'
    # let os.path.join handle the insertion of slashes
    return os.path.join(base_url, middle, suffix)


def _validate_http_url(url, args):
    """Validate that the http-url is reasonably well formed."""
    # Very similar to _validate_submodule_url() but lacking SSH support
    LOG.debug2("_validate_http_url() url: %s", url)
    have_error = False
    try:
        u = url.format(**args)
    except (ValueError, KeyError):
        have_error = True
    if not url.endswith('{repo}'):
        have_error = True
    m = not have_error and HTTP_URL_REGEX.match(u)
    if not m:
        LOG.error(_('Configuration setting perforce-to-git/http-url is malformed'))
        return None
    return u


def _format_http_url(repo_config):
    """Retrieve the appropriate URL for the given context."""
    # Very similar to _submodule_url() but lacking concern for SSH.
    url = repo_config.get(p4gf_config.SECTION_PERFORCE_TO_GIT, p4gf_config.KEY_HTTP_URL)
    if url is None:
        LOG.error('Git Fusion configuration missing perforce-to-git/http-url value.')
    else:
        args = {}
        args['user'] = 'git'  # fallback value
        for env_key in ['LOGNAME', 'USER', 'USERNAME']:
            if env_key in os.environ:
                args['user'] = os.environ[env_key]
                break
        args['host'] = socket.gethostname()
        args['repo'] = repo_config.repo_name
        url = _validate_http_url(url.strip(), args)
    LOG.debug2('_format_http_url() result: %s', url)
    return url


def _log_payload(msg, data):
    """Log the given payload using the appropriate level.

    :param str msg: first argument to logging.debug()
    :param data: payload to be logged, either length, prefix, or entire content

    """
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3(msg, data)
    elif LOG.isEnabledFor(logging.DEBUG2):
        LOG.debug2(msg, data[:256] + '...')
    elif LOG.isEnabledFor(logging.DEBUG):
        LOG.debug(msg, "len({0})".format(len(data)))


def _detect_client_and_version(user_agent):
    """Detect the client and version, raising an error if unsupported."""
    #
    # If there is a valid user-agent header, and if that agent appears to
    # be "git-lfs", then check that the version is one that is reasonably
    # bug-free. Currently that means at least 1.0.0 for git-lfs.
    #
    # All other agents are given the green light since we know nothing
    # about them and thus cannot rightly reject them.
    #
    if user_agent is not None and '/' in user_agent:
        agent, suffix = user_agent.split('/', 1)
        if agent == 'git-lfs':
            version = suffix.split(' ')[0]
            version_parts = version.split('.')
            try:
                version_nums = [int(v) for v in version_parts]
                if version_nums[0] < 1:
                    msg = _('unsupported version of git-lfs, please upgrade to 1.0.0 or higher')
                    raise p4gf_server_common.BadRequestException(msg)
            except ValueError:
                LOG.debug('cannot parse git-lfs client version: %s', version)


@with_timer('LFS/WSGI app')
def _wsgi_app(environ, start_response):
    """WSGI application to process the incoming Git client request.

    This is nearly equivalent to p4gf_auth_server.main() with the exception of
    input validation and error handling.
    """
    p4gf_version_3.log_version_extended()
    p4gf_util.log_environ(LOG, environ, "WSGI")
    p4gf_version_3.version_check()
    LOG.info("Processing LFS/HTTP request, pid=%s", os.getpid())
    result = p4gf_http_common.check_file_encoding(start_response)
    if result:
        return result
    _response = functools.partial(p4gf_http_common.send_error_response, start_response)
    try:
        input_file = p4gf_http_common.read_request_data(environ)
        server = LargeFileHttpServer(environ, start_response, input_file)
        try:
            server.process()
        except p4gf_server_common.BadRequestException as e:
            return _response(http.client.BAD_REQUEST, e)
        except p4gf_server_common.PerforceConnectionFailed:
            return _response(http.client.INTERNAL_SERVER_ERROR, _("Perforce connection failed\n"))
        except p4gf_server_common.CommandError as ce:
            return _response(http.client.FORBIDDEN, ce)
        except p4gf_server_common.ReadOnlyInstanceException as roie:
            return _response(http.client.FORBIDDEN, roie)
        except RuntimeError as rerr:
            return _response(http.client.INTERNAL_SERVER_ERROR, rerr)
    finally:
        p4gf_http_common.rm_file_quietly(input_file)

    return []


def app_wrapper(environ, start_response):
    """WSGI wrapper to our WSGI/CGI hybrid application.

    :param dict environ: WSGI environment
    :param callable start_response: function to initiate response

    """
    p4gf_http_common.wsgi_begin_request(environ)
    handler = functools.partial(_wsgi_app, environ, start_response)
    return p4gf_http_common.wsgi_run_request_handler(handler, start_response)


def _handle_cgi():
    """Respond to the incoming CGI request."""
    # Let the wsgiref module do the heavy lifting.
    if os.environ['PATH_INFO'] == '/' and 'SCRIPT_NAME' in os.environ:
        # Using ScriptAlias(Match) in Apache causes the PATH_INFO to be
        # truncated, and SCRIPT_NAME takes its place. Correct this since
        # much of our code expects PATH_INFO to include the portion of the
        # request URI that tells us the name of the repository.
        os.environ['PATH_INFO'] = os.environ['SCRIPT_NAME']
    handler = wsgiref.handlers.CGIHandler()
    handler.run(_wsgi_app)


@with_timer('LFS-HTTP main')
def main():
    """Parse command line arguments and decide what should be done."""
    desc = _("""p4gf_lfs_http_server.py handles LFS requests over HTTP.
Typically it is run via a web server and protected by some form of user
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
                        help=_('port on which to listen for LFS reqeuests'))
    args = parser.parse_args()
    if args.port:
        LOG.info("Listening for LFS-HTTP requests on port %s, pid=%s", args.port, os.getpid())
        httpd = wsgiref.simple_server.make_server('', args.port, app_wrapper)
        print(_('Serving on port {port}...').format(port=args.port))
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
        except Exception:  # pylint:disable=broad-except
            LOG.exception('HTTP main failed')
