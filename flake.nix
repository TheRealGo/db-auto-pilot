{
  description = "db-auto-pilot development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            python313
            uv
            nodejs_22
          ];

          shellHook = ''
            export DB_AUTO_PILOT_ROOT=$PWD
            export DB_AUTO_PILOT_DATA_DIR=''${DB_AUTO_PILOT_DATA_DIR:-$PWD/data}
            echo "db-auto-pilot shell ready"
            echo "python: $(python --version 2>/dev/null)"
            echo "uv: $(uv --version 2>/dev/null)"
            echo "node: $(node --version 2>/dev/null)"
          '';
        };
      });
}

