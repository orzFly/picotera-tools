# Toolchain shared by the devshell (.nix/flake.nix) and the package flake (../flake.nix).
{ pkgs }:
{
  go = pkgs.go_1_26;
  gopls = pkgs.gopls;
}
