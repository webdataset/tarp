# Read the testdata tar file, keep only the first 10 records
# ouputput them, and check that 10 records have been kept.

read -r result < <(
    $tarp cat --slice 0:10:2 testdata.tar -o - |
    tar tf - |
    fgrep .info.json |
    wc -l
)
test $result = 5 || echo $result
