import os
import subprocess
from datetime import datetime, timedelta
import json
from hashlib import sha1
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
import shutil
import inspect


def extended_json_parser(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return str(obj)
    raise TypeError(f"No custom parsing for {type(obj)} implemented.")


def get_cur_date_str():
    return datetime.now().strftime('%Y%m%d')


def get_unique_file_name(base_name):
    """Adds _{i} to the base_name until the file does not exist (i=1,2,3,...)."""
    base, suffix = base_name.rsplit(".", maxsplit=1)
    cur_append = ""
    cur_i = 0
    while os.path.exists(f"{base}{cur_append}.{suffix}"):
        cur_i += 1
        cur_append = f"_{cur_i}"
    return f"{base}{cur_append}.{suffix}"


def create_conda_yml(data_dir):
    # collect hashes of other yml files in this folder
    full_yml_paths = [os.path.join(data_dir, fp) for fp in os.listdir(data_dir) if fp.endswith('.yml')]
    other_hashes = [(fp, get_file_hash(fp)) for fp in full_yml_paths]
    conda_env_name = os.environ['CONDA_DEFAULT_ENV']
    print(conda_env_name)

    with NamedTemporaryFile() as tempfile:
        subprocess.Popen(['conda', 'env', 'export'], stdout=tempfile).communicate()
        cur_hash = get_file_hash(tempfile.name)
        try:
            idx = [oh[1] for oh in other_hashes].index(cur_hash)
            return other_hashes[idx][0]
        except ValueError:
            # does not exist
            export_path = get_unique_file_name(f"ENV_{conda_env_name}_{get_cur_date_str()}.yml")
            export_path = os.path.join(data_dir, export_path)
            shutil.copy2(tempfile.name, export_path)
            return export_path


def get_file_hash(file_path):
    hash = sha1()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(2**20), b""):
            hash.update(chunk)
    return hash.hexdigest()


def get_current_git_rev():
    # from numpy: https://stackoverflow.com/a/40170206/6499250
    def _minimal_ext_cmd(cmd):
        # construct minimal environment
        env = {}
        for k in ['SYSTEMROOT', 'PATH']:
            v = os.environ.get(k)
            if v is not None:
                env[k] = v
        # LANGUAGE is used on win32
        env['LANGUAGE'] = 'C'
        env['LANG'] = 'C'
        env['LC_ALL'] = 'C'
        out = subprocess.Popen(cmd, stdout=subprocess.PIPE, env=env).communicate()[0]
        return out

    try:
        out = _minimal_ext_cmd(['git', 'rev-parse', 'HEAD'])
        git_revision = out.strip().decode('ascii')
    except OSError:
        git_revision = "Unknown"

    return git_revision


@contextmanager
def export_artifact_meta(artifact_path):
    # assume that the file is created before contextmanager exits
    # generate following meta values
    #  - date and time (start / finish)
    #  - git revision
    #  - file hash
    #  - primary generating file

    start_time = datetime.now()

    try:
        git_rev = get_current_git_rev()
    except Exception as e:
        print("caught",e)

    artifact_dir = os.path.dirname(artifact_path)
    if artifact_dir == "":
        artifact_dir = "."

    yield

    # TODO be very careful here, losing any information due to an exception is very bad
    assert git_rev == get_current_git_rev()

    end_time = datetime.now()
    hash = get_file_hash(artifact_path)
    cwd = os.getcwd()

    frame_stack = inspect.stack()
    stack = [frame.filename for frame in frame_stack]
    del frame_stack  # to avoid reference cycles (https://stackoverflow.com/a/55469882/6499250)

    conda_yml_path = create_conda_yml(artifact_dir)

    # create output dict
    meta = {
        'start_time': start_time,
        'end_time': end_time,
        'duration': end_time - start_time,
        'file_sha1': hash,
        'working_dir': cwd,
        'call_stack': stack,
        'conda_yml': conda_yml_path
        }

    artifact_name = artifact_path.rsplit(".", maxsplit=1)[0]  # remove suffix
    json_target = get_unique_file_name(f"{artifact_name}.meta.json")
    with open(json_target, 'w') as json_file:
        json.dump(meta, json_file, default=extended_json_parser, indent=4, sort_keys=True)
