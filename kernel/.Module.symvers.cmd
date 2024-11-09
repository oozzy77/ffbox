cmd_/home/ec2-user/ffmount/Module.symvers :=  sed 's/ko$$/o/'  /home/ec2-user/ffmount/modules.order | scripts/mod/modpost -m -a     -o /home/ec2-user/ffmount/Module.symvers -e -i Module.symvers -T - 
