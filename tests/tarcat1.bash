# Read the testdata tar file, keep only the first 10 records
# ouputput them, and check that 10 records have been kept.

read -r result < <(
    $tarp cat --end 10 testdata.tar -o - |
    tar tf - |
    fgrep .info.json |
    wc -l
)
test $result = 10
