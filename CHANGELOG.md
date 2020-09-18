## [1.11.0-develop.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.10.1...v1.11.0-develop.1) (2020-09-18)


### :sparkles: Feature

* **handle_unknown_status:** Merge pull request [#126](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/126) from Cogility/fix/risk-doc-dups ([e57af6e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/e57af6ecc3cedc179c4f0756661b5df2a0afe4eb))

### [1.10.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.10.0...v1.10.1) (2020-09-17)


### :bug: Bugfix

* **redis empty list bug:** forgot not ([f491e7d](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/f491e7dc9694f9db9cb184165ffe03029b98efa7))
* **redis empty list bug:** Merge pull request [#125](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/125) from Cogility/hotfix/redix-empty-list-bug ([8ce502e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/8ce502eea6395f3ca0b3001f6030da991bcfd0b1))

## [1.10.0](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.9.1...v1.10.0) (2020-09-08)


### :sparkles: Feature

* **1.7.2:** Release, dev to master ([f51187e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/f51187ec7f674ae82b2dddd765242daa7b2be33b))
* **broadwayKafka producer:** Merge pull request [#120](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/120) from Cogility/feature/drilldown-ingestion-performance-updates ([62c5e55](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/62c5e5517da2c01aae139a703646155b3758b687))
* **CDST-444:** Merge pull request [#122](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/122) from Cogility/feature/CDST-444-rework-subscriptions ([4705028](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/47050285978ea4e5f41560bbb690025237e02c17))
* **CDST-682:** Merge pull request [#101](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/101) from Cogility/hotfix/resolve-consumer-notification-status ([42cd25a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/42cd25a04694d394306c2093f4372614781c7b8f))
* **more redis streams:** Merge pull request [#117](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/117) from Cogility/feature/redis-streams-event-producer ([cf32cc3](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/cf32cc35b8e5da6bdaaff7e6aa379a05f05ac16a))
* **redis streams:** Merge pull request [#115](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/115) from Cogility/feature/redis-streams-drilldown ([a196503](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/a1965037e726712d5e1ecf6ec2aafb79aff25674))


### :bug: Bugfix

* Merge pull request [#104](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/104) from Cogility/hotfix/switch-drilldown-back-to-cache ([aa5d084](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aa5d084f135fae1bb7238d88f20a2eb7738520bf))
* **elastic link event changes:** Merge pull request [#108](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/108) from Cogility/hotfix/update-link-event-processor ([a1d5da9](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/a1d5da9e959819bb78ae50a6b6517ffe93fbaac0))
* put changes back into master ([045e07a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/045e07afdf9d21d41843f2a11782dfb43b1d2170))
* **CDST-682:** Merge pull request [#102](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/102) from Cogility/hotfix/CDST-682-elastic-bulk-upsert ([2eb1d1a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/2eb1d1a6d4451cd3743bff48c28a82163837f37c))
* **debug:** Merge pull request [#110](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/110) from Cogility/hotfix/drilldown-processor-demand ([04448e7](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/04448e79fbed63cf80c19a755b748f35d207aa11))
* **drilldown:** Merge pull request [#100](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/100) from Cogility/hotfix/increase-drilldown-processors ([3625f1e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/3625f1e4ede6438ddbe6b5356a4be31a94cd1c3d))
* **drilldown:** Merge pull request [#109](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/109) from Cogility/hotfix/drilldown-cache-bug-fixes ([86382a0](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/86382a04603664c4013ce68109bbc33fa679acee))
* **drilldown debug:** Merge pull request [#111](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/111) from Cogility/hotfix/drilldown-dev-delete-debugging ([7f28fa7](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/7f28fa71e38b354043a2485308028e10270b3756))
* **drilldown debug:** Merge pull request [#114](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/114) from Cogility/hotfix/drilldown-consumer-groups-restart ([13ae005](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/13ae0054d5e3d17309ae020201ee7e8db155551a))
* **drilldown trap exit:** Merge pull request [#113](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/113) from Cogility/hotfix/drilldown-consumer-groups-restart ([ac55972](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/ac55972c22f78b10079acd777e12c2cc55d56d05))
* **ingest app restart:** Merge pull request [#107](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/107) from Cogility/hotfix/producer-keys-to-redis ([7ee7656](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/7ee765629269565c6d06ca916421cebd30ca5724))
* **major fix drilldown:** Merge pull request [#116](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/116) from Cogility/hotfix/CDST-693-drilldown-with-risk-fix ([b43afee](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/b43afee900e71b53a6424ed13cd701fb6f515fa5))
* **rate limiting:** Merge pull request [#106](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/106) from Cogility/hotfix/remove-broadway-rate-limiting ([aa871ee](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aa871eed9d771d3c2819669588403d5cb21e65c5))

## [1.10.0-develop.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.9.1...v1.10.0-develop.1) (2020-09-08)


### :sparkles: Feature

* **broadwayKafka producer:** Merge pull request [#120](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/120) from Cogility/feature/drilldown-ingestion-performance-updates ([62c5e55](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/62c5e5517da2c01aae139a703646155b3758b687))
* **CDST-444:** Merge pull request [#122](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/122) from Cogility/feature/CDST-444-rework-subscriptions ([4705028](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/47050285978ea4e5f41560bbb690025237e02c17))
* **CDST-682:** Merge pull request [#101](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/101) from Cogility/hotfix/resolve-consumer-notification-status ([42cd25a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/42cd25a04694d394306c2093f4372614781c7b8f))
* **more redis streams:** Merge pull request [#117](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/117) from Cogility/feature/redis-streams-event-producer ([cf32cc3](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/cf32cc35b8e5da6bdaaff7e6aa379a05f05ac16a))
* **redis streams:** Merge pull request [#115](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/115) from Cogility/feature/redis-streams-drilldown ([a196503](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/a1965037e726712d5e1ecf6ec2aafb79aff25674))


### :bug: Bugfix

* Merge pull request [#104](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/104) from Cogility/hotfix/switch-drilldown-back-to-cache ([aa5d084](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aa5d084f135fae1bb7238d88f20a2eb7738520bf))
* **elastic link event changes:** Merge pull request [#108](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/108) from Cogility/hotfix/update-link-event-processor ([a1d5da9](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/a1d5da9e959819bb78ae50a6b6517ffe93fbaac0))
* put changes back into master ([045e07a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/045e07afdf9d21d41843f2a11782dfb43b1d2170))
* **CDST-682:** Merge pull request [#102](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/102) from Cogility/hotfix/CDST-682-elastic-bulk-upsert ([2eb1d1a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/2eb1d1a6d4451cd3743bff48c28a82163837f37c))
* **debug:** Merge pull request [#110](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/110) from Cogility/hotfix/drilldown-processor-demand ([04448e7](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/04448e79fbed63cf80c19a755b748f35d207aa11))
* **drilldown:** Merge pull request [#100](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/100) from Cogility/hotfix/increase-drilldown-processors ([3625f1e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/3625f1e4ede6438ddbe6b5356a4be31a94cd1c3d))
* **drilldown:** Merge pull request [#109](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/109) from Cogility/hotfix/drilldown-cache-bug-fixes ([86382a0](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/86382a04603664c4013ce68109bbc33fa679acee))
* **drilldown debug:** Merge pull request [#111](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/111) from Cogility/hotfix/drilldown-dev-delete-debugging ([7f28fa7](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/7f28fa71e38b354043a2485308028e10270b3756))
* **drilldown debug:** Merge pull request [#114](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/114) from Cogility/hotfix/drilldown-consumer-groups-restart ([13ae005](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/13ae0054d5e3d17309ae020201ee7e8db155551a))
* **drilldown trap exit:** Merge pull request [#113](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/113) from Cogility/hotfix/drilldown-consumer-groups-restart ([ac55972](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/ac55972c22f78b10079acd777e12c2cc55d56d05))
* **ingest app restart:** Merge pull request [#107](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/107) from Cogility/hotfix/producer-keys-to-redis ([7ee7656](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/7ee765629269565c6d06ca916421cebd30ca5724))
* **major fix drilldown:** Merge pull request [#116](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/116) from Cogility/hotfix/CDST-693-drilldown-with-risk-fix ([b43afee](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/b43afee900e71b53a6424ed13cd701fb6f515fa5))
* **rate limiting:** Merge pull request [#106](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/106) from Cogility/hotfix/remove-broadway-rate-limiting ([aa871ee](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aa871eed9d771d3c2819669588403d5cb21e65c5))

## [1.7.0-develop.3](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.7.0-develop.2...v1.7.0-develop.3) (2020-08-31)


### :sparkles: Feature

* **CDST-444:** Merge pull request [#122](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/122) from Cogility/feature/CDST-444-rework-subscriptions ([4705028](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/47050285978ea4e5f41560bbb690025237e02c17))

## [1.7.0-develop.2](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.7.0-develop.1...v1.7.0-develop.2) (2020-08-27)


### :sparkles: Feature

* **broadwayKafka producer:** Merge pull request [#120](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/120) from Cogility/feature/drilldown-ingestion-performance-updates ([62c5e55](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/62c5e5517da2c01aae139a703646155b3758b687))

## [1.7.0-develop.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.10...v1.7.0-develop.1) (2020-08-20)


### :bug: Bugfix

* Merge pull request [#104](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/104) from Cogility/hotfix/switch-drilldown-back-to-cache ([aa5d084](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aa5d084f135fae1bb7238d88f20a2eb7738520bf))
* **elastic link event changes:** Merge pull request [#108](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/108) from Cogility/hotfix/update-link-event-processor ([a1d5da9](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/a1d5da9e959819bb78ae50a6b6517ffe93fbaac0))
* put changes back into master ([045e07a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/045e07afdf9d21d41843f2a11782dfb43b1d2170))
* **CDST-682:** Merge pull request [#102](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/102) from Cogility/hotfix/CDST-682-elastic-bulk-upsert ([2eb1d1a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/2eb1d1a6d4451cd3743bff48c28a82163837f37c))
* **debug:** Merge pull request [#110](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/110) from Cogility/hotfix/drilldown-processor-demand ([04448e7](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/04448e79fbed63cf80c19a755b748f35d207aa11))
* **drilldown:** Merge pull request [#100](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/100) from Cogility/hotfix/increase-drilldown-processors ([3625f1e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/3625f1e4ede6438ddbe6b5356a4be31a94cd1c3d))
* **drilldown:** Merge pull request [#109](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/109) from Cogility/hotfix/drilldown-cache-bug-fixes ([86382a0](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/86382a04603664c4013ce68109bbc33fa679acee))
* **drilldown debug:** Merge pull request [#111](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/111) from Cogility/hotfix/drilldown-dev-delete-debugging ([7f28fa7](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/7f28fa71e38b354043a2485308028e10270b3756))
* **drilldown debug:** Merge pull request [#114](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/114) from Cogility/hotfix/drilldown-consumer-groups-restart ([13ae005](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/13ae0054d5e3d17309ae020201ee7e8db155551a))
* **drilldown trap exit:** Merge pull request [#113](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/113) from Cogility/hotfix/drilldown-consumer-groups-restart ([ac55972](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/ac55972c22f78b10079acd777e12c2cc55d56d05))
* **ingest app restart:** Merge pull request [#107](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/107) from Cogility/hotfix/producer-keys-to-redis ([7ee7656](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/7ee765629269565c6d06ca916421cebd30ca5724))
* **major fix drilldown:** Merge pull request [#116](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/116) from Cogility/hotfix/CDST-693-drilldown-with-risk-fix ([b43afee](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/b43afee900e71b53a6424ed13cd701fb6f515fa5))
* **rate limiting:** Merge pull request [#106](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/106) from Cogility/hotfix/remove-broadway-rate-limiting ([aa871ee](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aa871eed9d771d3c2819669588403d5cb21e65c5))


### :sparkles: Feature

* **CDST-682:** Merge pull request [#101](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/101) from Cogility/hotfix/resolve-consumer-notification-status ([42cd25a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/42cd25a04694d394306c2093f4372614781c7b8f))
* **more redis streams:** Merge pull request [#117](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/117) from Cogility/feature/redis-streams-event-producer ([cf32cc3](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/cf32cc35b8e5da6bdaaff7e6aa379a05f05ac16a))
* **redis streams:** Merge pull request [#115](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/115) from Cogility/feature/redis-streams-drilldown ([a196503](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/a1965037e726712d5e1ecf6ec2aafb79aff25674))

## [1.6.0-develop.13](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.0-develop.12...v1.6.0-develop.13) (2020-08-19)


### :sparkles: Feature

* **more redis streams:** Merge pull request [#117](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/117) from Cogility/feature/redis-streams-event-producer ([cf32cc3](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/cf32cc35b8e5da6bdaaff7e6aa379a05f05ac16a))

## [1.6.0-develop.12](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.0-develop.11...v1.6.0-develop.12) (2020-08-18)


### :bug: Bugfix

* **downgrade versions:** Merge pull request [#123](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/123) from Cogility/hotfix/downgrade-models-and-migrations ([b8fbe21](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/b8fbe219fcfae2a2bf2bc8d8b6372cb3ebfab961))

## [1.9.0](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.8.1...v1.9.0) (2020-08-27)


### :sparkles: Feature

* **BroadwayKafka Producer:** dev -> master ([e7c48f4](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/e7c48f45395200ae72ab658aa4bd6b8890e4caf6))

### [1.8.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.8.0...v1.8.1) (2020-08-20)


### :bug: Bugfix

* **major fix drilldown:** Merge pull request [#116](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/116) from Cogility/hotfix/CDST-693-drilldown-with-risk-fix ([b7e893a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/b7e893a5dba73b48e83ebb964e87e8a6a38b6724))

## [1.8.0](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.7.0...v1.8.0) (2020-08-19)

### :sparkles: Feature

- **more redis streams:** Merge pull request [#117](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/117) from Cogility/feature/redis-streams-event-producer ([941e9a6](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/941e9a65844d513471cc9bebe28ff7a618001e92))

## [1.7.0](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.10...v1.7.0) (2020-08-18)

### :bug: Bugfix

- **drilldown debug:** Merge pull request [#114](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/114) from Cogility/hotfix/drilldown-consumer-groups-restart ([26b7751](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/26b7751f3f1eb381d5f70ba324c18c1d50faf85f))

### [1.6.9](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.8...v1.6.9) (2020-08-17)

### :bug: Bugfix

- **drilldown trap exit:** Merge pull request [#113](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/113) from Cogility/hotfix/drilldown-consumer-groups-restart ([577a696](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/577a69694e8685ddaad1833a1377c50e4012528a))

### [1.6.8](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.7...v1.6.8) (2020-08-17)

### :bug: Bugfix

- **drilldown debug:** Merge pull request [#111](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/111) from Cogility/hotfix/drilldown-dev-delete-debugging ([20dc84c](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/20dc84cab49281feb5cba66b0dfdda4dab73f9aa))

### [1.6.7](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.6...v1.6.7) (2020-08-16)

### :bug: Bugfix

- **debug:** Merge pull request [#110](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/110) from Cogility/hotfix/drilldown-processor-demand ([2afbba6](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/2afbba6f3433522b4a8cc6186cb5d6a0b8dce7ac))

### [1.6.6](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.5...v1.6.6) (2020-08-16)

### :bug: Bugfix

- **drilldown:** Merge pull request [#109](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/109) from Cogility/hotfix/drilldown-cache-bug-fixes ([7200fd2](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/7200fd23cf3a8db3a2b762eb7a1d9ca47b40c967))

### [1.6.5](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.4...v1.6.5) (2020-08-15)

### :bug: Bugfix

- **elastic link event changes:** Merge pull request [#108](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/108) from Cogility/hotfix/update-link-event-processor ([5375c43](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/5375c43c76eb9ec1d769524405271ce5cbe2ac17))

### [1.6.4](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.3...v1.6.4) (2020-08-14)

### :bug: Bugfix

- **ingest app restart:** Merge pull request [#107](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/107) from Cogility/hotfix/producer-keys-to-redis ([aeb769c](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aeb769c3548398dd8371fd10e517afaaebf4ead0))

### [1.6.3](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.2...v1.6.3) (2020-08-13)

### :bug: Bugfix

- **rate limiting:** Merge pull request [#106](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/106) from Cogility/hotfix/remove-broadway-rate-limiting ([de57121](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/de571210cf4676c3f99d39b83d5c9cefd5cba157))

### [1.6.2](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.1...v1.6.2) (2020-08-13)

### :bug: Bugfix

- Merge pull request [#105](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/105) from Cogility/hotfix/cherrypick-merge ([d339ab7](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/d339ab7804c073ee7b3553845f3034f492d54c85))

### [1.6.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.0...v1.6.1) (2020-08-11)

### :bug: Bugfix

- **CDST-682:** Merge pull request [#102](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/102) from Cogility/hotfix/CDST-682-elastic-bulk-upsert ([69feeec](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/69feeece757b7d6642d32ab333cddb8711d24b7a))

## [1.6.0](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.4...v1.6.0) (2020-08-11)

### :sparkles: Feature

- **CDST-682:** cherry-pick changes ([1b3165e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/1b3165e1fa962019a1b5a1fee01fac0a46c92491))

### [1.5.4](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.3...v1.5.4) (2020-08-06)

### :bug: Bugfix

- **update_notification:** trigger build ([e3b012a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/e3b012aa7c47743cc1bf3c544f9849764a8a224f))

### [1.5.3](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.2...v1.5.3) (2020-08-05)

### :bug: Bugfix

- **manual-actions:** trigger build ([0129465](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/01294653529dc004838ad4cd180477ccb0df9d02))

### [1.5.2](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.1...v1.5.2) (2020-08-04)

### :bug: Bugfix

- **rate limiting:** Merge pull request [#106](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/106) from Cogility/hotfix/remove-broadway-rate-limiting ([de57121](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/de571210cf4676c3f99d39b83d5c9cefd5cba157))

### [1.6.2](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.1...v1.6.2) (2020-08-13)

### :bug: Bugfix

* **drilldown:** Merge pull request [#100](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/100) from Cogility/hotfix/increase-drilldown-processors ([3625f1e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/3625f1e4ede6438ddbe6b5356a4be31a94cd1c3d))
* **rate limiting:** Merge pull request [#106](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/106) from Cogility/hotfix/remove-broadway-rate-limiting ([aa871ee](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aa871eed9d771d3c2819669588403d5cb21e65c5))

## [1.6.0-develop.3](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.0-develop.2...v1.6.0-develop.3) (2020-08-13)


### :bug: Bugfix

* Merge pull request [#104](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/104) from Cogility/hotfix/switch-drilldown-back-to-cache ([aa5d084](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/aa5d084f135fae1bb7238d88f20a2eb7738520bf))

## [1.6.0-develop.2](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.6.0-develop.1...v1.6.0-develop.2) (2020-08-11)

- **CDST-682:** Merge pull request [#102](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/102) from Cogility/hotfix/CDST-682-elastic-bulk-upsert ([69feeec](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/69feeece757b7d6642d32ab333cddb8711d24b7a))

## [1.6.0](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.4...v1.6.0) (2020-08-11)

### :sparkles: Feature

- **CDST-682:** cherry-pick changes ([1b3165e](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/1b3165e1fa962019a1b5a1fee01fac0a46c92491))

### [1.5.4](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.3...v1.5.4) (2020-08-06)

### :bug: Bugfix

* **CDST-682:** Merge pull request [#102](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/102) from Cogility/hotfix/CDST-682-elastic-bulk-upsert ([2eb1d1a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/2eb1d1a6d4451cd3743bff48c28a82163837f37c))

## [1.6.0-develop.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.3-develop.1...v1.6.0-develop.1) (2020-08-11)

### :bug: Bugfix

- **manual-actions:** trigger build ([0129465](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/01294653529dc004838ad4cd180477ccb0df9d02))

### [1.5.2](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.1...v1.5.2) (2020-08-04)

### :bug: Bugfix

- Merge pull request [#97](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/97) from Cogility/hotfix/update-manual-actions-attr ([14d0485](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/14d0485a89ca99e56774a693f8def57d7d0c3824))
- update manual actions expected attribute ([1588420](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/1588420652f5d71393b2c0866c1a95aaf7fee3b4))

### [1.5.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.0...v1.5.1) (2020-08-03)

### :bug: Bugfix

- **livenessCheck hotfix:** change default kafka worker ([829a353](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/829a353b0175f69ccec49293115c46475a0ee841))

## [1.5.0](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.4.5...v1.5.0) (2020-08-03)

### :sparkles: Feature

* **CDST-682:** Merge pull request [#101](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/101) from Cogility/hotfix/resolve-consumer-notification-status ([42cd25a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/42cd25a04694d394306c2093f4372614781c7b8f))

### [1.5.3-develop.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.5.2...v1.5.3-develop.1) (2020-08-05)

### :bug: Bugfix

* put changes back into master ([045e07a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/045e07afdf9d21d41843f2a11782dfb43b1d2170))

### [1.4.5-develop.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.4.4...v1.4.5-develop.1) (2020-07-30)

### :bug: Bugfix

* put changes back into master ([045e07a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/045e07afdf9d21d41843f2a11782dfb43b1d2170))

### [1.4.4-develop.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.4.3...v1.4.4-develop.1) (2020-07-30)

### :bug: Bugfix

* **color:** remove from master ([eee8c56](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/eee8c5660274ccabd34cf3a03c49aae33a1076f1))
* put changes back into master ([045e07a](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/045e07afdf9d21d41843f2a11782dfb43b1d2170))

### [1.4.3-develop.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.4.2...v1.4.3-develop.1) (2020-07-30)

### [1.4.3](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.4.2...v1.4.3) (2020-07-30)

* **start drilldown consumer:** Merge pull request [#94](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/94) from Cogility/hotfix/start-drilldown-consumer ([2d67a0d](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/2d67a0d60110ceb2d07352cb7ba2d25a8c4e153e))

### [1.4.2](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.4.1...v1.4.2) (2020-07-29)

### :bug: Bugfix

- force deps update ([ceccd08](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/ceccd080be2c56beb66e8687f5936aa44197b160))

### [1.4.1](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.4.0...v1.4.1) (2020-07-29)

### :bug: Bugfix

- hardcode redis port ([3dde0ce](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/3dde0ce8d194d51d6c732186ecafc0debf41c307))

## [1.4.0](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.3.25...v1.4.0) (2020-07-27)

### :bug: Bugfix

- update audit topic name ([c882220](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/c8822208eeea7fa0bf8385961d90fb14e199c655))
- updated naming ([2fabf87](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/2fabf87980a99a0de6f6f214717d0f5b6106c2b3))
- **lmetrics deps:** remove dev only ([d477ab7](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/d477ab7f8e1c72536d88c6eb34480eb88994fd5b))

### :sparkles: Feature

- **Release 1.5.5:** Merge pull request [#90](https://github.com/Cogility/cogynt-ws-ingest-otp/issues/90) from Cogility/develop ([e566df2](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/e566df2f2d473236a635bef1c8623c86d401d6d8))

### [1.3.22-develop.2](https://github.com/Cogility/cogynt-ws-ingest-otp/compare/v1.3.22-develop.1...v1.3.22-develop.2) (2020-07-08)

### :bug: Bugfix

- updated naming ([2fabf87](https://github.com/Cogility/cogynt-ws-ingest-otp/commit/2fabf87980a99a0de6f6f214717d0f5b6106c2b3))

### [1.3.22-develop.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.21...v1.3.22-develop.1) (2020-07-06)

### :bug: Bugfix

- update audit topic name ([c882220](https://github.com/Cogility/cogynt-workstation-ingest/commit/c8822208eeea7fa0bf8385961d90fb14e199c655))

### [1.3.21-develop.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.20...v1.3.21-develop.1) (2020-06-16)

### :bug: Bugfix

- **delete data:** Merge pull request [#78](https://github.com/Cogility/cogynt-workstation-ingest/issues/78) from Cogility/develop ([5828b2e](https://github.com/Cogility/cogynt-workstation-ingest/commit/5828b2e95c4f6009d24797fb0932304cfbed236a))

### [1.3.20](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.19...v1.3.20) (2020-06-15)

### :bug: Bugfix

- **status change:** Merge pull request [#77](https://github.com/Cogility/cogynt-workstation-ingest/issues/77) from Cogility/develop ([3fea805](https://github.com/Cogility/cogynt-workstation-ingest/commit/3fea80565e98965eedb3fe1b3a96f002e9476be0))

### [1.3.19](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.18...v1.3.19) (2020-06-15)

### :bug: Bugfix

- **check if processing:** Merge pull request [#76](https://github.com/Cogility/cogynt-workstation-ingest/issues/76) from Cogility/develop ([1178c76](https://github.com/Cogility/cogynt-workstation-ingest/commit/1178c762fdc13cde58d09c1f8ca876516814a75c))

### [1.3.18](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.17...v1.3.18) (2020-06-15)

### :bug: Bugfix

- Nill state for consumer without existing topic ([74403e8](https://github.com/Cogility/cogynt-workstation-ingest/commit/74403e8904b382c891bbb5f99f23386ae64feea8))

### [1.3.17](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.16...v1.3.17) (2020-06-13)

### :bug: Bugfix

- Merge pull request [#73](https://github.com/Cogility/cogynt-workstation-ingest/issues/73) from Cogility/develop ([99896e3](https://github.com/Cogility/cogynt-workstation-ingest/commit/99896e3ff174b906a47d372355a2449efb39bc7f))

### [1.3.16](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.15...v1.3.16) (2020-06-12)

### :bug: Bugfix

- **condition check:** Merge pull request [#72](https://github.com/Cogility/cogynt-workstation-ingest/issues/72) from Cogility/hotfix/add-error-catch ([c590522](https://github.com/Cogility/cogynt-workstation-ingest/commit/c5905225d2a462a86b7e45e01349370a83a18ce3))

### [1.3.15](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.14...v1.3.15) (2020-06-11)

### :bug: Bugfix

- Merge pull request [#71](https://github.com/Cogility/cogynt-workstation-ingest/issues/71) from Cogility/develop ([cf4d73e](https://github.com/Cogility/cogynt-workstation-ingest/commit/cf4d73e1dd1139290d1bafabfeb55af86ed07f81))

### [1.3.14](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.13...v1.3.14) (2020-06-10)

### :bug: Bugfix

- **nsetting checks:** Merge pull request [#70](https://github.com/Cogility/cogynt-workstation-ingest/issues/70) from Cogility/develop ([b6fbf15](https://github.com/Cogility/cogynt-workstation-ingest/commit/b6fbf15391a5952fe55bdc1419afe3955339320d))

### [1.3.13](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.12...v1.3.13) (2020-06-10)

### :bug: Bugfix

- **CDST-568:** Merge pull request [#68](https://github.com/Cogility/cogynt-workstation-ingest/issues/68) from Cogility/feature/CDST-568-backfill-notification-rework ([46881f0](https://github.com/Cogility/cogynt-workstation-ingest/commit/46881f04b703ed2dbb49268610babcc611ba75e7))
- Merge pull request [#69](https://github.com/Cogility/cogynt-workstation-ingest/issues/69) from Cogility/develop ([b889516](https://github.com/Cogility/cogynt-workstation-ingest/commit/b889516cd57bac47ac028f63383f815fd2e250d7))

### :hammer: Build

- updated to use build-harness ([112e153](https://github.com/Cogility/cogynt-workstation-ingest/commit/112e1536351b7f2064189c8bac935ad5cf520a2e))

### [1.3.13-develop.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.12...v1.3.13-develop.1) (2020-06-10)

### :bug: Bugfix

- **CDST-568:** Merge pull request [#68](https://github.com/Cogility/cogynt-workstation-ingest/issues/68) from Cogility/feature/CDST-568-backfill-notification-rework ([46881f0](https://github.com/Cogility/cogynt-workstation-ingest/commit/46881f04b703ed2dbb49268610babcc611ba75e7))

### :hammer: Build

- updated to use build-harness ([112e153](https://github.com/Cogility/cogynt-workstation-ingest/commit/112e1536351b7f2064189c8bac935ad5cf520a2e))

### [1.3.12-develop.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.11...v1.3.12-develop.1) (2020-06-09)

### :hammer: Build

- updated to use build-harness ([112e153](https://github.com/Cogility/cogynt-workstation-ingest/commit/112e1536351b7f2064189c8bac935ad5cf520a2e))

### [1.3.11](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.10...v1.3.11) (2020-06-04)

### :bug: Bugfix

- **CDST-547 system tag index:** Merge pull request [#66](https://github.com/Cogility/cogynt-workstation-ingest/issues/66) from Cogility/hotfix/CDST-547-update-migrations-and-models-packages ([de369f8](https://github.com/Cogility/cogynt-workstation-ingest/commit/de369f87473eeb690efc11ed58a5455d131a9466))

### [1.3.10](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.9...v1.3.10) (2020-06-04)

### :bug: Bugfix

- **missing search:** Merge pull request [#65](https://github.com/Cogility/cogynt-workstation-ingest/issues/65) from Cogility/potential-fix/incorrect-elastic-doc-creation ([978ab13](https://github.com/Cogility/cogynt-workstation-ingest/commit/978ab135cf6c05ed7c786e883a2b4ac752c5760d))

### [1.3.9](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.8...v1.3.9) (2020-06-03)

### :bug: Bugfix

- **notifications:** Merge pull request [#63](https://github.com/Cogility/cogynt-workstation-ingest/issues/63) from Cogility/develop ([abf6a9f](https://github.com/Cogility/cogynt-workstation-ingest/commit/abf6a9f37aca43bc9dc24315a0ba43d52ac41b4f))

### [1.3.8-develop.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.7...v1.3.8-develop.1) (2020-06-02)

### :bug: Bugfix

- **update configs:** Merge pull request [#62](https://github.com/Cogility/cogynt-workstation-ingest/issues/62) from Cogility/fix(parse-string)-Convert-strings-to-int-in-config ([0a05c3c](https://github.com/Cogility/cogynt-workstation-ingest/commit/0a05c3c2b2d1343501dd8ef3c4d0bfb47eb45eab))

### [1.3.7](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.6...v1.3.7) (2020-06-02)

### :bug: Bugfix

- **Merge pull request #58 from Cogility/develop:** livness ([855edb9](https://github.com/Cogility/cogynt-workstation-ingest/commit/855edb9739e584e17d747e6ce4432606d660969e)), closes [#58](https://github.com/Cogility/cogynt-workstation-ingest/issues/58)

### [1.3.6](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.5...v1.3.6) (2020-06-01)

### :bug: Bugfix

- **helm:** removed resource limits. ([88c15eb](https://github.com/Cogility/cogynt-workstation-ingest/commit/88c15eb2423bf5c2160779291d2cc3abffadd161))

### [1.3.5](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.4...v1.3.5) (2020-05-29)

### :bug: Bugfix

- **Release:** Release ([02c8965](https://github.com/Cogility/cogynt-workstation-ingest/commit/02c8965e61eed27b60ee483263d5a21cc938ca3b))

### [1.3.4](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.3...v1.3.4) (2020-05-27)

### :bug: Bugfix

- **drilldown cahce:** Merge pull request [#56](https://github.com/Cogility/cogynt-workstation-ingest/issues/56) from Cogility/develop ([d2f984e](https://github.com/Cogility/cogynt-workstation-ingest/commit/d2f984ed772e92b378b95fe6801ebc808c93b1fc))

## [1.3.0-develop.2](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.0-develop.1...v1.3.0-develop.2) (2020-05-21)

### :bug: Bugfix

- **disable_auth:** disabling auth for drilldown ([6c47731](https://github.com/Cogility/cogynt-workstation-ingest/commit/6c47731f4659dc9608e3ea6178f2749095e6fc39))

### [1.3.2](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.1...v1.3.2) (2020-05-21)

### :bug: Bugfix

- **build:** route to new drilldown ([05349bd](https://github.com/Cogility/cogynt-workstation-ingest/commit/05349bd3104a9225ee6d74c6796fea16116ad239))

### [1.3.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.3.0...v1.3.1) (2020-05-21)

### :bug: Bugfix

- **helm:** statefulset pvt labels ([a091daa](https://github.com/Cogility/cogynt-workstation-ingest/commit/a091daa09ad7ce2408e3e6dfa8965d7f805a28eb))

## [1.3.0](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.2.22...v1.3.0) (2020-05-21)

### :sparkles: Feature

- **Release 1.4:** Merge pull request [#54](https://github.com/Cogility/cogynt-workstation-ingest/issues/54) from Cogility/develop ([04ce76a](https://github.com/Cogility/cogynt-workstation-ingest/commit/04ce76a26103166087f517312c53dc33ce089bf1))
- **Release 1.4:** Release for initial AF env ([01705fb](https://github.com/Cogility/cogynt-workstation-ingest/commit/01705fba1331857a76dfb5731715c1ed5cd6ec3f))

### :repeat: CI

- added git branch var for version ([f0d1181](https://github.com/Cogility/cogynt-workstation-ingest/commit/f0d1181c661de8a5af11d093ee94cdad355132cf))

## [1.3.0-develop.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.2.22-develop.1...v1.3.0-develop.1) (2020-05-20)

### :sparkles: Feature

- **Release 1.4:** Release for initial AF env ([01705fb](https://github.com/Cogility/cogynt-workstation-ingest/commit/01705fba1331857a76dfb5731715c1ed5cd6ec3f))

### [1.2.22-develop.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.2.21...v1.2.22-develop.1) (2020-05-19)

### :repeat: CI

- added git branch var for version ([f0d1181](https://github.com/Cogility/cogynt-workstation-ingest/commit/f0d1181c661de8a5af11d093ee94cdad355132cf))

### [1.2.17-develop.1](https://github.com/Cogility/cogynt-workstation-ingest/compare/v1.2.16...v1.2.17-develop.1) (2020-05-14)

### :repeat: CI

- added git branch var for version ([f0d1181](https://github.com/Cogility/cogynt-workstation-ingest/commit/f0d1181c661de8a5af11d093ee94cdad355132cf))

### :bug: Bugfix

- **configs:** Adding new kafka_ex configs ([0e9aa55](https://github.com/Cogility/cogynt-workstation-ingest/commit/0e9aa55f6cd7d7a49d232a93c8f8666252a8f10c))
