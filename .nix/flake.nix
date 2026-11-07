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
      in
      {
        devShell = pkgs.devshell.mkShell {
          imports = [{
            name = "devshell";
            packages = [
              pkgs.nixpkgs-fmt
              pkgs.go
              pkgs.gopls
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
