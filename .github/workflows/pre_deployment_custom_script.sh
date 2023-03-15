# Option custom processing script
# This will run in the Github runner VM after the code is checked out but before deploying to S3
# So git repo is available to you as well as some of the git environment variables
# Can not take any arguments currently


echo "Pre-deployment custom script"
pwd
ls -ltr