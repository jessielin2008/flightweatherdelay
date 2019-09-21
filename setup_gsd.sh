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

#############################################################
#
# Download weather dataset from NOAA and copy to HDFS 
# THIS TAKES OVER 10 HOURS DON'T RUN IT IF YOU DON'T HAVE TO
#
#############################################################
## make dir for models
mkdir -p models/spark

## make directory and remove older files
mkdir -p ~/data/isd
cd ~/data/isd

## download weather files for 2004-2007 in gz format
for i in $(seq 2006 2007)
do 
  ## download weather file for each year
  wget ftp://ftp.ncdc.noaa.gov/pub/data/noaa/$i/*-$i.gz
  
  ## unzip them to text file
  gzip -d *.gz
  
  ## combine files into a single large file
  array=($(ls *$i))
  file_name=(isd-working.txt)
  echo "filename is $file_name"
  
  for file in ${array[@]};
  do
    #concatinate them to a big file
    echo "concatenate $file to $file_name"
    cat $file >> $file_name
  done
  echo "file for year $i combined."
  
  ## remove working files
  rm *-$i
  mv isd-working.txt isd-$i.txt
done

## get airport reference file
wget ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv

## copy weather data from local container to hdfs weather folder
#hdfs dfs -mkdir flightdelay
hdfs dfs -mkdir flightdelay/weatherhourly
hdfs dfs -copyFromLocal ~/data/*.txt flightdelay/weatherhourly
hdfs dfs -copyFromLocal ~/data/isd-history.csv flightdelay/weatherhourly
echo "weather files copied to HDFS."

#rm ~/data/*.txt
#rm ~/data/isd-history.csv

#rm ~/data/*.*
#rmdir ~/data 
