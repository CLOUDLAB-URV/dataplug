make clean
make html
rm -rf /tmp/dataplug-docs
mkdir -p /tmp/dataplug-docs
cp -R build/html/* /tmp/dataplug-docs
git checkout docs