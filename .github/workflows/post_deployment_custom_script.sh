# Option custom processing script
# This will run in the Github runner VM after the code is checked out and deployed to S3
# So git repo is available to you as well as some of the git environment variables
# Can not take any arguments currently


echo "post-deployment custom script"
pwd
ls -ltr