read -r result < <(
    $tarp cat --end 10 testdata.tar -o - |
    tar tf - |
    fgrep .info.json |
    wc -l
)
test $result = 10
