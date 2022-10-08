{ pkgs ? import <nixpkgs> { } }:

with pkgs;


mkShell {
  buildInputs = [
    go_1_18
    gotools
    gopls
    czmq
    libsodium
    # go-outline
    # gocode
    # gopkgs
    # gocode-gomod
    # godef
    # golint
  ];
  shellHook =
    ''
      echo "use make to build"
    '';
}
