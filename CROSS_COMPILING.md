# CROSS COMPILING

To prepare the compilation environment for an ARM64 architecture such as the Raspberry Pi 3B+ (64 bits OS) the steps are several.

First of all, you need to enable `sudo dpkg --add-architecture arm64`.

Then to install `zlib1g-dev:arm64` on an x86 machine you need to update the `/etc/apt/sources.list` file manually (this works only on ubuntu `jammy` or lower).

The lines to be added are:

```
deb [arch=amd64] http://archive.ubuntu.com/ubuntu <version> main restricted universe multiverse
deb [arch=amd64] http://archive.ubuntu.com/ubuntu <version>-updates main restricted universe multiverse
deb [arch=amd64] http://archive.ubuntu.com/ubuntu <version>-security main restricted universe multiverse
```

Remember to **Restore the file to its previous state once you installed it**, so careful if you do not know what you are doing.

There are two scripts, one to install all the dependencies (which assumes the steps above have been completed):

```bash
sudo ./cross_compile.sh
# And
sudo ./cleanup.sh
```

`sudo` is required for installing packages and for adding the wasmedge libraries to `/usr/local`.
