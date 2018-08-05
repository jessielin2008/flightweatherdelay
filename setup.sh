#############################################################
#
# Download weather dataset from NOAA and copy to HDFS 
#
#############################################################

## make directory and remove older files
mkdir ~/data
cd ~/data
rm *.*

## get airport reference file
wget ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv

## download weather files for 2004-2007 in tar format
for i in $(seq 2004 2007)
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
    echo "concatenate $file to $file_name"
    cat $file >> $file_name
  done
  echo "file for year $i combined."
  
  ## remove working files
  rm *$i.op
done


## copy weather data from local container to hdfs weather folder
hdfs dfs -mkdir flightdelay
hdfs dfs -mkdir flightdelay/weather
hdfs dfs -copyFromLocal ~/data/*.txt flightdelay/weather
hdfs dfs -copyFromLocal ~/data/isd-history.csv flightdelay/weather
echo "weather files copied to HDFS."

rm ~/data/*.tar
rm ~/data/*.txt
rm ~/data/isd-history.csv


#############################################################
#
# Download flight dataset from Stat Computing and copy to HDFS 
#
#############################################################

## remove files and start from clean state
cd ~/data
rm *.*

## download files for 2004-2007
for i in $(seq 2004 2007)
do 
  ## download file for each year
  wget http://stat-computing.org/dataexpo/2009/$i.csv.bz2
  
  ## untar and unzip them to *.op file
  bzip2 -d *.bz2
  
done

## copy data from local container to hdfs folder
hdfs dfs -mkdir flightdelay/flights
hdfs dfs -copyFromLocal ~/data/*.csv flightdelay/flights
echo "flight files copied to HDFS."

rm ~/data/*.csv

#rm ~/data/*.*
rmdir ~/data 
