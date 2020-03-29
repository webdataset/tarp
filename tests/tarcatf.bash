fields=foo:info.json
$tarp cat -f $fields testdata.tar -o - | tar tf - | fgrep .foo > /dev/null
$tarp cat -f $fields testdata.tar -o - | tar tf - | fgrep -v .png > /dev/null

