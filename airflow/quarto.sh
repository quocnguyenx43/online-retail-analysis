#!/bin/bash

curl -L -o ~/quarto-1.7.34-linux-amd64.tar.gz \
    https://github.com/quarto-dev/quarto-cli/releases/download/v1.7.34/quarto-1.7.34-linux-amd64.tar.gz
mkdir ~/opt
tar -C ~/opt -xvzf ~/quarto-1.7.34-linux-amd64.tar.gz

mkdir ~/.local/bin
ln -s ~/opt/quarto-1.7.34/bin/quarto ~/.local/bin/quarto

( echo ""; echo 'export PATH=$PATH:~/.local/bin\n' ; echo "" ) >> ~/.profile
source ~/.profile
