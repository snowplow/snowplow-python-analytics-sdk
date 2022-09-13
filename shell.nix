{ pkgs ? import <nixpkgs> {}}:

pkgs.mkShell {
  buildInputs = [ pkgs.python38 ];
  shellHook = ''
    # export SOURCE_DATE_EPOCH=315532800 # 1980
    # python -m venv .venv
    # source .venv/bin/activate
    # pip3 install --trusted-host pypi.org --trusted-host files.pythonhosted.org wheel -r requirements.txt -r requirements-dev.txt packaging
    # export PYTHONPATH=.:./layer/python
    # export MYPYPATH=$PYTHONPATH
 '';
}
