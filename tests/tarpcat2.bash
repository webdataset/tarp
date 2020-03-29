read -r result < <(
    $tarp cat -f png testdata.tar testdata.tar -o - | tar tf - | wc -l
)
test $result = 40

