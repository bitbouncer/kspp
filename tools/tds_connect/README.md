wget ftp://ftp.freetds.org/pub/freetds/stable/freetds-patched.tar.gz
tar xvf freetds-patched.tar.gz
cd freetds-1.00.86
./configure
make -j8
sudo make install
sudo cp -r include/freetds /usr/local/include




http://www.freetds.org/userguide/confirminstall.htm#TSQL
src/apps/tsql.c

sudo apt-get install unixodbc unixodbc-dev freetds-dev freetds-bin tdsodbc


 tsql -H hostmachine -p port -U username -P password


