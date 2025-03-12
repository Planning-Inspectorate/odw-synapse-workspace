export DEBIAN_FRONTEND=noninteractive
export DEBIAN_PRIORITY=critical

sudo echo 'APT::Acquire::Retries "3";' > /etc/apt/apt.conf.d/80-retries
sudo echo "APT::Get::Assume-Yes \"true\";" > /etc/apt/apt.conf.d/90assumeyes

sudo add-apt-repository main
sudo add-apt-repository restricted
sudo add-apt-repository universe
sudo add-apt-repository multiverse
sudo add-apt-repository ppa:git-core/ppa
sudo add-apt-repository ppa:deadsnakes/ppa

sudo sed -i -e 's/^# deb-src/deb-src/' /etc/apt/sources.list

sudo apt-get update
sudo apt-get clean && sudo apt-get upgrade -y

sudo apt-get install -y --no-install-recommends \
  apt-transport-https \
  build-essential \
  dpkg-dev \
  libapt-pkg-dev \
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
  zip \
  git \
  git-lfs \
  git-ftp \
  python3.12 \
  python3.12-dev \
  python3-apt \
  python3-distutils \
  python3-setuptools

sudo ln -sf /usr/bin/python3.12 /usr/bin/python3
echo "==================== PYTHON DEFAULT VERSION ===================="
python3 --version
echo "================================================================"

sudo chown -R _apt /home/packer

# Automated Validation for apt_pkg
python3 -c "import apt_pkg" || {
  echo "apt_pkg not found. Attempting to rebuild python3-apt..." | tee -a apt_pkg_error.log
  sudo apt-get source python3-apt || echo "Failed to fetch source for python3-apt" | tee -a apt_pkg_error.log
  cd python-apt-* || echo "Failed to navigate to python3-apt source directory" | tee -a apt_pkg_error.log
  python3.12 setup.py build || echo "Failed to build python3-apt" | tee -a apt_pkg_error.log
  sudo python3.12 setup.py install || echo "Failed to install python3-apt" | tee -a apt_pkg_error.log
  cd ..
}

python3 -c "import apt_pkg" || echo "Error: apt_pkg still not found after rebuild" | tee -a apt_pkg_error.log

curl -sL https://aka.ms/InstallAzureCLIDeb | bash

sudo curl -fsSL https://aka.ms/install-azd.sh | bash

wget https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb

sudo apt-get update
sudo apt-get install -y aspnetcore-runtime-6.0 powershell

pwsh -c "& {Install-Module -Name Az -Scope AllUsers -Repository PSGallery -Force -Verbose}"
pwsh -c "& {Get-Module -ListAvailable}"

/usr/sbin/waagent -force -deprovision+user && export HISTSIZE=0 && sync
