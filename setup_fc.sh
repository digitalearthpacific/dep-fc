
. .venv/bin/activate
export PATH="/app/.venv/bin:$PATH"
git clone --depth=1 https://github.com/GeoscienceAustralia/fc.git
cd fc
git pull
python setup.py build
python setup.py install
cmake .
cmake --build .
cp unmiximage.so fc/unmix/
cd ..
uv run python -c "from fc import fractional_cover"
