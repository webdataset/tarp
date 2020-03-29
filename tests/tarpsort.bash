read -r result < <(
    $tarp sort -f png testdata.tar -o - | tar tf - | wc -l
)
test $result = 20

