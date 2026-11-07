{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    flake-utils.url = "github:numtide/flake-utils";
    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = inputs@{ self, ... }:
    inputs.flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ inputs.devshell.overlays.default ];
        };
        common = import ./common.nix { inherit pkgs; };
      in
      {
        devShell = pkgs.devshell.mkShell {
          imports = [{
            name = "devshell";
            packages = [
              pkgs.nixpkgs-fmt
              common.go
              common.gopls
              pkgs.git-filter-repo
            ];
            commands = [
              {
                name = "ptt";
                command = ''
                  exec go run ./cmd/ptt "$@"
                '';
              }
            ];
          }];
        };
      });
}
