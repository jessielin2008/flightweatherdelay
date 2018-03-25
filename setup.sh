#############################################################
#
# Download weather dataset from NOAA and copy to HDFS 
#
#############################################################

## download weather data file from ncdc
cd Data
rm *.tar
rm *.op
rm *.txt

## get airport reference file
#wget ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv

## download weather files for 1987-2008 in tar format
for i in $(seq 1988 1988)
do 
  ## download weather file for each year
  wget ftp://ftp.ncdc.noaa.gov/pub/data/gsod/$i/gsod_$i.tar
  
  ## untar and unzip them to *.op file
  tar -xvf gsod_$i.tar
  gzip -d *.gz
  
  ## combine files into a single large file
  array=($(ls *$i.op))
  file_name=(gsod_$i.txt)
  echo "filename is $file_name"
  
  for file in ${array[@]};
  do
    #remover header
    sed -i '1d' $file
    #concatinate them to a big file
    echo "concatinate $file to $file_name"
    cat $file >> $file_name
  done
  echo "file for year $i combined."
  
  ## remove working files
  rm *$i.op
done

http://stat-computing.org/dataexpo/2009/1987.csv.bz2

## copy weather data from local container to hdfs weather folder
hdfs dfs -mkdir weather
hdfs dfs -copyFromLocal *.txt weather
hdfs dfs -copyFromLocal isd-history.csv weather
echo "files copied to HDFS."

rm *.txt
rm isd-history.csv


#############################################################
#
# Download dataset from Stat Computing and copy to HDFS 
#
#############################################################

## download weather data file from ncdc
cd Data
rm *.bz2

## download weather files for 1987-2008 in tar format
for i in $(seq 1988 2008)
do 
  ## download weather file for each year
  wget http://stat-computing.org/dataexpo/2009/$i.csv.bz2
  
  ## untar and unzip them to *.op file
  bzip2 -d *.bz2
  
done

## copy weather data from local container to hdfs weather folder
##hdfs dfs -mkdir flights
hdfs dfs -copyFromLocal *.csv flights
echo "flight files copied to HDFS."