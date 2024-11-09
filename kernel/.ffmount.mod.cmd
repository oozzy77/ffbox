cmd_/home/ec2-user/ffmount/ffmount.mod := printf '%s\n'   ffmount.o | awk '!x[$$0]++ { print("/home/ec2-user/ffmount/"$$0) }' > /home/ec2-user/ffmount/ffmount.mod
