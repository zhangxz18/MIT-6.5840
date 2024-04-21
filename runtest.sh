# rm ~/temp/raft_*

# for i in {1..40}; do
#   echo "Running test 2A iteration $i"
#   go test -run 2A >> ~/temp/raft_2A
#   if [ $? -ne 0 ]; then
#     echo "Test failed on iteration 2A $i"
#     break
#   fi
# done

# for i in {1..40}; do
#   echo "Running test 2B iteration $i"
#   go test -run 2B >> ~/temp/raft_2B
#   if [ $? -ne 0 ]; then
#     echo "Test failed on iteration 2B $i"
#     break
#   fi
# done

for i in {1..100}; do
  echo "Running test 2C iteration $i"
  go test -run 2C > ~/temp/raft_2C
  if [ $? -ne 0 ]; then
    echo "Test failed on 2C iteration $i"
    break
  fi
done

# for i in {1..40}; do
#   echo "Running test 2D iteration $i"
#   go test -run 2D >> ~/temp/raft_2D
#   if [ $? -ne 0 ]; then
#     echo "Test failed on 2D iteration $i"
#     break
#   fi
# done