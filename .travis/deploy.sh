echo 'RUNNING DEPLOY.SH'

cat > ~/.pypirc <<EOF
[distutils]
index-servers=
    pypi

[pypi]
repository = https://pypi.python.org/pypi
username = snowplow
password = $PYPI_PASSWORD

EOF

python setup.py sdist register upload
