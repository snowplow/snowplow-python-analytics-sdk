{
  description = "";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    flake-utils.inputs.nixpkgs.follows = "nixpkgs";
    mach-nix.url = "mach-nix/3.5.0";
    mach-nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, mach-nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
            config.allowUnsupportedSystem = true;
          };
      in {
        defaultPackage = mach-nix.lib."${system}".mkPythonShell {
          python = "python38";
          requirements = builtins.readFile ./requirements.txt;
          ignoreDataOutdated = true;
        };
      }
    );
}
