# Test transmission of tar records from a sender to
# a receiver via ZMQ push-pull
# Note that zpush:/zpull: can simply be used in place
# of regular input/output files with tarp cat.

rm -f _test-zmq.tar sender.log receiver.log
port=9559
coproc receiver {
    debug=receiver.log $tarp cat -L 1 -o _test-zmq.tar zpull://127.0.0.1:$port
}
sleep 3
debug=sender.log $tarp cat testdata.tar -L 1 -o zpush://127.0.0.1:$port
wait $receiver_PID
read result < <(
    tar tvf _test-zmq.tar | fgrep .info.json | wc -l
)
test $result = 20
