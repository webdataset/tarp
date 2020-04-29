# Read the testdata tar file, keep only the first 10 records
# ouputput them, and check that 10 records have been kept.

set -x

cat > _plan <<EOF
a.txt /dev/null
a.more.txt text:more
b.txt pipe:echo b
b.more.txt text:more2
EOF

$tarp create -o - _plan | tar tvf - | grep '  0 .*a.txt' > /dev/null
$tarp create -o - _plan | tar tvf - | grep '  4 .*a.more.txt' > /dev/null
$tarp create -o - _plan | tar tvf - | grep '  5 .*b.more.txt' > /dev/null
