export DEBIAN_FRONTEND=noninteractive
export DEBIAN_PRIORITY=critical

sudo echo 'APT::Acquire::Retries "3";' > /etc/apt/apt.conf.d/80-retries
sudo echo "APT::Get::Assume-Yes \"true\";" > /etc/apt/apt.conf.d/90assumeyes

# Add repositories
sudo add-apt-repository main
sudo add-apt-repository restricted
sudo add-apt-repository universe
sudo add-apt-repository multiverse
sudo add-apt-repository ppa:git-core/ppa
sudo add-apt-repository ppa:deadsnakes/ppa

# Enable source repositories for rebuilding python3-apt
sudo sed -i -e 's/^# deb-src/deb-src/' /etc/apt/sources.list

# Update and upgrade system
sudo apt-get update
sudo apt-get clean && sudo apt-get upgrade -y

# Install common dependencies
sudo apt-get install -y --no-install-recommends \
  apt-transport-https \
  build-essential \
  ca-certificates \
  software-properties-common \
  git git-lfs git-ftp \
  python3.12 python3.12-dev python3-apt python3-distutils

# Set Python 3.12 as default
sudo ln -sf /usr/bin/python3.12 /usr/bin/python3
echo "==================== PYTHON DEFAULT VERSION ===================="
python3 --version
echo "================================================================"

# Verify apt_pkg module
python3 -c "import apt_pkg" || {
  echo "apt_pkg not found, rebuilding python3-apt..."
  sudo apt-get source python3-apt
  cd python-apt-*
  python3.12 setup.py build
  sudo python3.12 setup.py install
  cd ..
}

# Disable cnf-update-db temporarily to avoid errors
sudo chmod -x /usr/lib/cnf-update-db

# Verify apt_pkg after rebuilding
python3 -c "import apt_pkg" || echo "Error: apt_pkg still not found"

# Re-enable cnf-update-db
sudo chmod +x /usr/lib/cnf-update-db

# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Install Azure Developer Tools
sudo curl -fsSL https://aka.ms/install-azd.sh | bash

# Install .NET Core and PowerShell
wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb

sudo apt-get update
sudo apt-get install -y aspnetcore-runtime-6.0 powershell

# Install PowerShell modules
pwsh -c "& {Install-Module -Name Az -Scope AllUsers -Repository PSGallery -Force -Verbose}"
pwsh -c "& {Get-Module -ListAvailable}"

# Sysprep to prepare the VM for deployment
/usr/sbin/waagent -force -deprovision+user && export HISTSIZE=0 && sync
