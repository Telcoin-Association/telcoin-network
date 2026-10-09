"""Assemble bounded official metadata and local source pins, never touch archives."""
import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

BASE = None
EVIDENCE = None
API_INPUTS = None
REPO = None
HEAD = '3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b'
TREE = '6175a752c787eef23cfefe76324a735d5f2baef2'
RUN = 37990580614
JOB = 114023405270
PREFIX = 'repos/Telcoin-Association/telcoin-network/'
ATTEMPTS = []


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def put(path, raw):
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes()!=raw:
            raise ValueError('existing metadata input differs: '+str(path))
        return {'path':str(path),'sha256':sha(raw),'bytes':len(raw)}
    with path.open('xb') as stream:
        stream.write(raw)
    return {'path':str(path),'sha256':sha(raw),'bytes':len(raw)}


def git(*args):
    return subprocess.run(['git','--no-replace-objects','-C',str(REPO),*args],
                          check=True,capture_output=True,timeout=30).stdout


def fetch(name, endpoint, limit=4*1024*1024):
    attempt={'name':name,'endpoint':endpoint,'method':'GET','started_at':datetime.now(timezone.utc).isoformat()}
    ATTEMPTS.append(attempt)
    retained=API_INPUTS/name
    if retained.exists():
        raw=retained.read_bytes()
        if len(raw)>limit:
            raise ValueError('retained official response exceeds bound')
        attempt.update(exit_code=0,status='reused successful first-attempt response',
                       completed_at=datetime.now(timezone.utc).isoformat(),**put(retained,raw))
        return raw
    result=subprocess.run(['gh','api','--allow-escape-sequences','--method','GET',PREFIX+endpoint],capture_output=True,timeout=60)
    attempt.update(exit_code=result.returncode,bytes=len(result.stdout),stderr_bytes=len(result.stderr),
                   completed_at=datetime.now(timezone.utc).isoformat())
    if result.returncode or len(result.stdout)>limit or len(result.stderr)>8192:
        attempt['stderr']=result.stderr[:8192].decode(errors='replace')
        put(BASE/'assembly-get-failure.json',
            (json.dumps(attempt,sort_keys=True,indent=2)+'\n').encode())
        raise ValueError('bounded official GET failed: '+name)
    attempt.update(put(API_INPUTS/name,result.stdout))
    return result.stdout


def configure(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('repo', 'evidence-dir', 'api-inputs', 'run-metadata', 'jobs-metadata',
                 'artifacts-metadata', 'artifact-metadata', 'source-pins', 'verifier',
                 'prior-preparation', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args(argv)
    global REPO, EVIDENCE, API_INPUTS, BASE, RUN_METADATA, JOBS_METADATA
    global ARTIFACTS_METADATA, ARTIFACT_METADATA, SOURCE_PINS, VERIFIER, PRIOR_PREPARATION, OUTPUT
    REPO = args.repo.resolve(strict=True)
    EVIDENCE = args.evidence_dir.resolve(strict=True)
    API_INPUTS = args.api_inputs.absolute()
    RUN_METADATA = args.run_metadata.resolve(strict=True)
    JOBS_METADATA = args.jobs_metadata.resolve(strict=True)
    ARTIFACTS_METADATA = args.artifacts_metadata.resolve(strict=True)
    ARTIFACT_METADATA = args.artifact_metadata.resolve(strict=True)
    SOURCE_PINS = args.source_pins.resolve(strict=True)
    VERIFIER = args.verifier.resolve(strict=True)
    PRIOR_PREPARATION = args.prior_preparation.resolve(strict=True)
    OUTPUT = args.output.absolute()
    BASE = OUTPUT.parent.resolve(strict=True)


def main():
    API_INPUTS.mkdir(exist_ok=True)
    files=[]
    def copy(name,source,expected):
        raw=source.read_bytes()
        if sha(raw)!=expected:
            raise ValueError('sealed input changed: '+str(source))
        target=EVIDENCE/name
        item=put(target,raw)
        files.append({'name':name,'source_path':str(source),'derivation':'byte-identical sealed official response',**item})
        return json.loads(raw)
    run=copy('run.json',RUN_METADATA,'ab6f9667602c7358a98eca1716f89977d88025aa7cec71ee80d33ccee1664ad5')
    jobs=copy('jobs.json',JOBS_METADATA,'550526df4dd1a93ae11ba72cc6161df9b22e2535489e07a7c2a6553761273645')
    artifacts=copy('artifacts-list.json',ARTIFACTS_METADATA,'87876ba251e092217cd570251ed59e1674dbe0c7b64de9663dc58b0a47dd2455')
    artifact=copy('artifact-metadata-official.json',ARTIFACT_METADATA,'701de1996a6272b0ba9a52bba3da517b9b02876da3d38c3f8c75467a02f26088')
    assert run['id']==RUN and run['head_sha']==HEAD and run['run_attempt']==1 and run['conclusion']=='success'
    assert jobs['total_count']==1 and len(jobs['jobs'])==1 and jobs['jobs'][0]['id']==JOB
    assert jobs['jobs'][0]['conclusion']=='success'
    assert artifacts['total_count']==1 and artifacts['artifacts'][0]['id']==artifact['id']==11645521069
    job_raw=(json.dumps(jobs['jobs'][0],sort_keys=True,indent=2)+'\n').encode()
    files.append({'name':'job.json','derivation':'complete sole job object from sealed official jobs response',
                  'source_path':str(JOBS_METADATA),**put(EVIDENCE/'job.json',job_raw)})
    endpoints=[('source-commit-official.json','commits/'+HEAD),
               ('merge-recursive-tree-v2.json','git/trees/'+TREE+'?recursive=1'),
               ('pr.json','pulls/1502'),('staging-ref.json','git/ref/heads/staging/mavenrain-2026-10-09'),
               ('job.log','actions/jobs/'+str(JOB)+'/logs')]
    responses={name:fetch(name,endpoint) for name,endpoint in endpoints}
    source=json.loads(responses['source-commit-official.json'])
    tree=json.loads(responses['merge-recursive-tree-v2.json'])
    pr=json.loads(responses['pr.json']);staging=json.loads(responses['staging-ref.json'])
    assert source['sha']==HEAD and source['commit']['tree']['sha']==TREE
    assert [x['sha'] for x in source['parents']]==['a45a21df90b60939f14ae1336bfa64585570043f']
    assert tree['sha']==TREE and tree['truncated'] is False
    assert pr['head']['sha']==HEAD and pr['base']['ref']=='staging/mavenrain-2026-10-09'
    assert pr['base']['sha']==staging['object']['sha']=='07d6feaa86475479993475fe3fdf368eabc695b7'
    for name,raw in responses.items():
        files.append({'name':name,'source_path':str(API_INPUTS/name),
                      'derivation':'byte-identical bounded official GET response',**put(EVIDENCE/name,raw)})
    files.append({'name':'merge-rest-commit.json','source_path':str(API_INPUTS/'source-commit-official.json'),
                  'derivation':'byte-identical current source commit alias',
                  **put(EVIDENCE/'merge-rest-commit.json',responses['source-commit-official.json'])})
    pins=json.loads(SOURCE_PINS.read_text())
    syntax=ast.parse(VERIFIER.read_text())
    source_paths=next(ast.literal_eval(n.value) for n in syntax.body if isinstance(n,ast.Assign)
                      and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='SOURCE_PATHS')
    path_expression=next(n.value for n in ast.walk(syntax) if isinstance(n,ast.Assign)
                         and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='paths')
    selected=eval(compile(ast.Expression(path_expression),'<source path closure only>','eval'),
                  {'FULL_SOURCE_FILES':set(pins['full_paths']),'SOURCE_PATHS':source_paths})
    entries={x['path']:x for x in tree['tree']}
    bound={}
    for path in selected:
        raw=git('show',HEAD+':'+path)
        blob_sha=hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
        assert entries[path]['type']=='blob' and entries[path]['sha']==blob_sha
        assert (REPO/path).read_bytes()==raw
        bound[path]={'git_blob_sha1':blob_sha,'sha256':sha(raw)}
    assert len(bound)==80
    old_report_raw=PRIOR_PREPARATION.read_bytes()
    assert sha(old_report_raw)=='8efabd5306f33894a7ee2445baa1d6b7ca9e73b661058007b003e3a19c65e542'
    old_report=json.loads(old_report_raw)
    capture={'version':1,'kind':'metadata-only source preparation','packet_id':'25bbef50cf9573cd2bdc930f',
             'head_sha':HEAD,'run_id':RUN,'job_id':JOB,'api_attempts':ATTEMPTS,'inputs':files,
             'official_tree_bound_blobs':bound,'integration_provenance_reused':old_report['integration'],
             'integration_metadata_note':'Historical integration evidence and original response timestamps remain unchanged. Current source commit aliases describe current source only.',
             'archive_operations_by_agent':False,'reader_main_executed':False,
             'archive_acquisition_pending':True}
    capture_pin=put(EVIDENCE/'capture-attempts.json',(json.dumps(capture,sort_keys=True,indent=2)+'\n').encode())
    files.append({'name':'capture-attempts.json','derivation':'source-only capture and metadata manifest',**capture_pin})
    manifest={'version':1,'kind':'source-only official QUIC input assembly','head_sha':HEAD,'run_id':RUN,'job_id':JOB,
              'required_metadata_inputs_complete':True,'files':files,'api_attempts':ATTEMPTS,
              'official_tree_bound_blobs':80,'tree_bound_source_manifest':capture_pin,
              'ordered_source_parents':['a45a21df90b60939f14ae1336bfa64585570043f'],
              'integration_provenance_reused':old_report['integration'],
              'current_staging_ref_name':'staging/mavenrain-2026-10-09',
              'current_staging_ref_sha':staging['object']['sha'],
              'archive_path_stat_or_read_by_this_agent':False,'archive_operations_by_this_agent':False,
              'reader_main_executed':False,'archive_acquisition_pending':True}
    sealed=put(OUTPUT,(json.dumps(manifest,sort_keys=True,indent=2)+'\n').encode())
    print(json.dumps({'manifest':sealed,'metadata_files':len(files),'tree_bound_blobs':len(bound),'official_GETs':len(ATTEMPTS)}))


if __name__=='__main__':
    configure()
    main()
