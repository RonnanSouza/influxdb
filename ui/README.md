## Packages

### Adding new packages
To add a new package, run

```sh
yarn add packageName
```

### Adding devDependency

```sh
yarn add packageName --dev
```

### Updating a package
First, run

```sh
yarn outdated
```

... to determine which packages may need upgrading.

We _really_ should not upgrade all packages at once, but, one at a time and make darn sure
to test.

To upgrade a single package named `packageName`:

```sh
yarn upgrade packageName
```

## Testing
Tests can be run via command line with `yarn test`, from within the `/ui` directory. For more detailed reporting, use `yarn test -- --reporters=verbose`.
