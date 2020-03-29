read -r result < <(
    $tarp proc -c 'touch sample.foo' testdata.tar -o - | tar tf - | fgrep .foo | wc -l
)
test $result = 20
