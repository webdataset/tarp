rm -f _test-*.tar __test-*.tar
$tarp split -o "_test-%02d.tar" -c 10 testdata.tar -p 'touch _%s'
test -f _test-00.tar
test -f _test-01.tar
! test -f _test-02.tar
test -f __test-00.tar
test -f __test-01.tar
rm -f _test-*.tar __test-*.tar

