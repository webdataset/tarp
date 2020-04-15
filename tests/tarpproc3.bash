# Verify that removing files inside a tar proc command are absent
# from the output.
read -r result < <(
$tarp proc -m 'for i in {0..9}; do (echo -n "$i-"; cat sample.__key__) | tee sample-000$i.__key__ > sample-000$i.txt; done' testdata.tar -o - | tar tf - | fgrep 1-283524.txt | wc -l
)
test $result = 1
