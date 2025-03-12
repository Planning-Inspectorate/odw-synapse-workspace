export DEBIAN_FRONTEND=noninteractive
export DEBIAN_PRIORITY=critical

sudo echo 'APT::Acquire::Retries "3";' > /etc/apt/apt.conf.d/80-retries
sudo echo "APT::Get::Assume-Yes \"true\";" > /etc/apt/apt.conf.d/90assumeyes

sudo add-apt-repository main
sudo add-apt-repository restricted
sudo add-apt-repository universe
sudo add-apt-repository multiverse
sudo apt-get update

sudo apt-get clean && sudo apt-get update && sudo apt-get upgrade
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

sudo add-apt-repository ppa:git-core/ppa
sudo add-apt-repository ppa:deadsnakes/ppa

# Git Installation
sudo apt-get install -y --no-install-recommends \
  git \
  git-lfs \
  git-ftp

# Install Python 3.12 and python3-apt
sudo apt-get install -y --no-install-recommends \
  python3.12 \
  python3.12-dev \
  python3-setuptools \
  python3-apt

# Rebuild python3-apt to link with Python 3.12
sudo apt-get source python3-apt
cd python-apt-*
python3.12 setup.py build
sudo python3.12 setup.py install
cd ..

# Verify apt_pkg
python3 --version
python3 -c "import apt_pkg" || echo "Error: apt_pkg not found"

# Azure CLI Installation
curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Azure Tools Installation
sudo curl -fsSL https://aka.ms/install-azd.sh | bash

# Install .NET Core and PowerShell
wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb

sudo apt-get update
sudo apt-get install -y aspnetcore-runtime-6.0
sudo apt-get install -y powershell

# Install PowerShell Modules
pwsh -c "& {Install-Module -Name Az -Scope AllUsers -Repository PSGallery -Force -Verbose}"
pwsh -c "& {Get-Module -ListAvailable}"

# Sysprep to Prepare the VM
/usr/sbin/waagent -force -deprovision+user && export HISTSIZE=0 && sync
