# Verify that removing files inside a tar proc command are absent
# from the output.
read -r result < <(
    $tarp proc -c 'rm *png*' testdata.tar -o - | tar tf - | fgrep .png | wc -l
)
test $result = 0

