export DEBIAN_FRONTEND=noninteractive

sudo echo 'APT::Acquire::Retries "3";' > /etc/apt/apt.conf.d/80-retries
sudo echo "APT::Get::Assume-Yes \"true\";" > /etc/apt/apt.conf.d/90assumeyes

sudo add-apt-repository main
sudo add-apt-repository restricted
sudo add-apt-repository universe
sudo add-apt-repository multiverse
sudo add-apt-repository ppa:git-core/ppa
sudo add-apt-repository ppa:deadsnakes/ppa

# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install dependencies
sudo apt-get install -y --no-install-recommends \
  apt-transport-https \
  build-essential \
  ca-certificates \
  software-properties-common \
  git git-lfs git-ftp \
  python3.12 python3.12-dev python3-apt

# Set Python 3.12 as default
sudo ln -sf /usr/bin/python3.12 /usr/bin/python3
python3 --version

# Rebuild python3-apt if apt_pkg is missing
python3 -c "import apt_pkg" || {
  sudo sed -i -e 's/^# deb-src/deb-src/' /etc/apt/sources.list
  sudo apt-get update
  sudo apt-get source python3-apt
  cd python-apt-*
  python3.12 setup.py build
  sudo python3.12 setup.py install
  cd ..
}

# Verify apt_pkg
python3 -c "import apt_pkg" || echo "Error: apt_pkg still not found"

# Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# .NET Core and PowerShell
wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt-get update
sudo apt-get install -y aspnetcore-runtime-6.0 powershell

# Sysprep
/usr/sbin/waagent -force -deprovision+user && export HISTSIZE=0 && sync
