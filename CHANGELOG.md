# Changelog

## [2.1.0](https://github.com/EIGSEP/eigsep_redis/compare/v2.0.0...v2.1.0) (2026-04-20)


### ⚠ BREAKING CHANGES

* rename Transport._upload_dict to upload_dict

### Code Refactoring

* rename Transport._upload_dict to upload_dict ([86ec362](https://github.com/EIGSEP/eigsep_redis/commit/86ec3627c6665dbd90d8d15140f129388acc32f5))

## [2.0.0](https://github.com/EIGSEP/eigsep_redis/compare/v1.0.0...v2.0.0) (2026-04-20)


### ⚠ BREAKING CHANGES

* remove EigsepRedis add_metadata shim
* parameterize HeartbeatWriter/Reader with name=

### Features

* parameterize HeartbeatWriter/Reader with name= ([f1c1958](https://github.com/EIGSEP/eigsep_redis/commit/f1c1958c7c953eff2e1f5eefd397b7bf9b1ae5e8))


### Documentation

* add claude.md ([322d6cc](https://github.com/EIGSEP/eigsep_redis/commit/322d6ccd9742d827a765248ed7695f79e1dd0f4e))


### Code Refactoring

* remove EigsepRedis add_metadata shim ([ad8d1d8](https://github.com/EIGSEP/eigsep_redis/commit/ad8d1d88e9b3077590c897cf96df9addcc87fdad))

## [1.0.0](https://github.com/EIGSEP/eigsep_redis/compare/v0.1.0...v1.0.0) (2026-04-20)


### ⚠ BREAKING CHANGES

* split EigsepRedis into Transport + writer/reader classes
* callers using corr/VNA methods must switch from EigsepRedis to EigsepObsRedis. eigsep_observing.EigsepRedis now resolves to the base bus class (no corr/VNA methods).

### Features

* **panda-client:** bump status stream maxlen 5 → 100 ([903a380](https://github.com/EIGSEP/eigsep_redis/commit/903a380955282c421997ba378a45c568d0190a98))
* **redis:** warn on stale metadata snapshot reads ([#62](https://github.com/EIGSEP/eigsep_redis/issues/62)) ([188e641](https://github.com/EIGSEP/eigsep_redis/commit/188e641be903ef8503b0798fd04fddfce43af52c))
* **redis:** warn on stale metadata stream drains ([77acb41](https://github.com/EIGSEP/eigsep_redis/commit/77acb41b68906ef82bdd4cfdf47d0bf50c078fe7))


### Bug Fixes

* restrict add_metadata to json serializable objects only ([8b89bcc](https://github.com/EIGSEP/eigsep_redis/commit/8b89bcc3bbba04328c27f2765f4c17f76282ed67))
* seperate metadata streams from data streams in EigRedis class ([1ab6548](https://github.com/EIGSEP/eigsep_redis/commit/1ab6548dad7c78865c7c696ffb8128ac427fcf30))


### Code Refactoring

* split bus primitives into shared eigsep_redis package ([549d67e](https://github.com/EIGSEP/eigsep_redis/commit/549d67e757c8d2f824f75460891a6addf2be8f2b))
* split EigsepRedis into Transport + writer/reader classes ([278787a](https://github.com/EIGSEP/eigsep_redis/commit/278787adc6f26237584366c3390ee6c3e25ba38b))
