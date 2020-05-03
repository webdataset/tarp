# Read the testdata tar file, keep only the first 10 records
# ouputput them, and check that 10 records have been kept.

(
echo testdata.tar
echo testdata.tar
echo testdata.tar
) > _list

read -r result < <(
    $tarp cat -m 2 -l _list -o - |
    tar tf - |
    fgrep .info.json |
    wc -l
)
test $result = 60
