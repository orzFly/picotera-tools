{
  inputs = {
    # The dev flake owns every real pin; this flake follows its lock graph.
    dev.url = "path:./.nix";
    nixpkgs.follows = "dev/nixpkgs";
  };

  # No flake-parts: hosts consume this flake as an input, and every extra input is one more
  # fetch on their side.
  outputs = inputs@{ self, nixpkgs, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAll (pkgs:
        let
          common = import ./.nix/common.nix { inherit pkgs; };
          version = "git-${self.shortRev or self.dirtyShortRev or "unknown"}";
          src = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [ ./go.mod ./go.sum ./cmd ./internal ];
          };
          build = name: (pkgs.buildGoModule.override { go = common.go; }) {
            pname = name;
            inherit version src;
            subPackages = [ "cmd/${name}" ];
            vendorHash = "sha256-WUqtimWESoDYzp8Ct7hMACWeLvxFmCqZ6UgrYnzdnJs=";
            env.CGO_ENABLED = 0;
            ldflags = [ "-s" "-w" ];
            meta.mainProgram = name;
          };
        in
        {
          pt-store = build "pt-store";
          ptt = build "ptt";
        });
    };
}
