. .venv/bin/activate
export PATH="/app/.venv/bin:$PATH"
git clone --depth=1 https://github.com/jessjaco/fc.git
git checkout leak-fix
cd fc
git pull
python setup.py build
python setup.py install
cmake .
cmake --build .
cp unmiximage.so fc/unmix/
cd ..
