#!/bin/bash

MESPDIR=$(pwd)
# Check the OS version and install according packages manually
echo "Checking OS version requirements..."
echo ""
echo ""
debian_version=`egrep "jessie|stretch" /etc/os-release`
if [ `echo $debian_version|grep -c "jessie"` == 1  ]
then
    echo "No Kafka support provided for Raspbian Jessie"
elif [ `echo $debian_version|grep -c "stretch"` == 1  ]
then
    echo "Manually installing librdkafka dependencies..."
    echo ""
    echo ""
    wget -P /tmp/ http://ftp.de.debian.org/debian/pool/main/libr/librdkafka/librdkafka1_0.11.6-1~bpo9+1_armhf.deb
    wget -P /tmp/ http://ftp.de.debian.org/debian/pool/main/libr/librdkafka/librdkafka++1_0.11.6-1~bpo9+1_armhf.deb
    wget -P /tmp/ http://ftp.de.debian.org/debian/pool/main/libr/librdkafka/librdkafka-dev_0.11.6-1~bpo9+1_armhf.deb
    cd /tmp/
    LIBRDKAFKA=$(sudo dpkg -i librdkafka1_0.11.6-1~bpo9+1_armhf.deb)
    if [ "" == "$LIBRDKAFKA"  ]; then
        echo "Cannot install librdkafka1_0.11.6. Aborting..."
        exit -1
    fi
    LIBRDKAFKAPP=$(sudo dpkg -i librdkafka++1_0.11.6-1~bpo9+1_armhf.deb)
    if [ "" == "$LIBRDKAFKAPP"  ]; then
        echo "Cannot install librdkafka++1_0.11.6. Aborting..."
        exit -1
    fi
    LIBRDKAFKADEV=$(sudo dpkg -i librdkafka-dev_0.11.6-1~bpo9+1_armhf.deb)
    if [ "" == "$LIBRDKAFKADEV"  ]; then
        echo "Cannot install librdkafka-dev_0.11.6. Aborting..."
        exit -1
    fi

else
    echo "No support provided for this version!"
    echo "Please upgrade either to Jessie or Stretch"
    exit -1

fi


# Installing header files for systemd
echo "Installing header files for libsystemd, libatlas-base and libhdf5..."
echo ""
echo ""
sudo apt install -y libsystemd-dev libatlas-base-dev  libhdf5-dev

# Print the Kernel version
kernel_version=`uname -a|awk '{print $3}'`
if [ $kernel_version == "4.4"  ]
then
    echo "Kernel version: " $kernel_version
else
    echo "Kernel version: " $kernel_version
fi

# Install requirements with pip
echo "Installing python requirements with pip..."
echo ""
echo ""
cd $MESPDIR
for package in $(cat requirements.txt); do
    sudo pip install $package
    if [ $? -ne 0  ]; then
        echo "Could not install $package. Aborting..."
        exit 1
    fi
done

# Install MESP as a systemd service
# Edit Unit file
echo "Creating Unit file start agent.py as a Systemd service..."
echo ""
echo ""
CMD=$MESPDIR"/py/agent.py -f "$MESPDIR"/conf/agent.ini"

# Add the user under who the service will run
STR1=`awk -v var="$USER" '{if(/User=/) {print$0var}}' $MESPDIR/conf/agent.service`
sed -i "s:User=:$STR1:g" $MESPDIR/conf/agent.service


# Adding command to be excecuted by systemd
STR2=`awk -v var="$CMD" '{if(/ExecStart=/) {print$0var}}' $MESPDIR/conf/agent.service`
echo $STR2
sed -i "s:ExecStart=:$STR2:g" $MESPDIR/conf/agent.service

# Link Unit file to systemd dir
echo "Linking file "$MESPDIR"/py/agent.py to systemd directory"
chmod 664 conf/agent.service
sudo ln -sf $MESPDIR/conf/agent.service /etc/systemd/system/agent.service
sudo systemctl daemon-reload
echo "Done!"
