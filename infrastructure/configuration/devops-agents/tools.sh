#!/bin/bash
# Set environment variables for non-interactive installations
export DEBIAN_FRONTEND=noninteractive
export DEBIAN_PRIORITY=critical

# Configure APT to retry on failure and assume "yes" for all prompts
sudo echo 'APT::Acquire::Retries "3";' > /etc/apt/apt.conf.d/80-retries
sudo echo "APT::Get::Assume-Yes \"true\";" > /etc/apt/apt.conf.d/90assumeyes

# Add standard Ubuntu repositories if needed
sudo add-apt-repository main
sudo add-apt-repository restricted
sudo add-apt-repository universe
sudo add-apt-repository multiverse

# Enable source repositories for fetching source packages
sudo sed -i -e 's/^# deb-src/deb-src/' /etc/apt/sources.list

# Update package lists and upgrade the system
sudo apt-get update
sudo apt-get clean && sudo apt-get update && sudo apt-get upgrade -y

# Install common dependencies and tools
sudo apt-get install -y --no-install-recommends \
  apt-transport-https \
  build-essential \
  ca-certificates \
  unixodbc-dev \
  curl \
  gnupg \
  jq \
  libasound2 \
  libgbm-dev \
  libgconf-2-4 \
  libgtk2.0-0 \
  libgtk-3-0 \
  libnotify-dev \
  libnss3 \
  libxss1 \
  libxtst6 \
  lsb-release \
  software-properties-common \
  unzip \
  wget \
  xauth \
  xvfb \
  zip

# Add external repositories
sudo add-apt-repository ppa:git-core/ppa
sudo add-apt-repository ppa:deadsnakes/ppa

# Install Git and related tools
sudo apt-get install -y --no-install-recommends \
  git \
  git-lfs \
  git-ftp

# Install Python 3.12, its development headers, and apt package module
sudo apt-get install -y --no-install-recommends \
  python3.12 \
  python3.12-dev \
  python3-setuptools \
  python3-apt

# Set Python 3.12 as the default Python version
sudo ln -sf /usr/bin/python3.12 /usr/bin/python3

# Fetch and rebuild the python3-apt package for Python 3.12
sudo apt-get source python3-apt
cd python-apt-*
python3.12 setup.py build
sudo python3.12 setup.py install
cd ..

# Verify Python version and the apt_pkg module
python3 --version
python3 -c "import apt_pkg" || echo "Error: apt_pkg not found"

# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Install Azure Developer Tools
sudo curl -fsSL https://aka.ms/install-azd.sh | bash

# Install .NET Core and PowerShell
wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb

sudo apt-get update
sudo apt-get install -y aspnetcore-runtime-6.0
sudo apt-get install -y powershell

# Install PowerShell modules
pwsh -c "& {Install-Module -Name Az -Scope AllUsers -Repository PSGallery -Force -Verbose}"
pwsh -c "& {Get-Module -ListAvailable}"

# Prepare the VM for deployment
/usr/sbin/waagent -force -deprovision+user && export HISTSIZE=0 && sync
