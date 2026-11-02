# Azure Service Bus ATOM management API: wire spec derived from the Go and JS SDKs

Scope: what an emulator's HTTP management endpoint must accept and return so that both
official admin clients work:

- **Go**: `azservicebus` v1.11.0 (`admin` + `internal/atom`), as pinned in `go.mod`. Go paths
  below are relative to the root of that module.
- **JS**: `@azure/service-bus` 7.10.0 + `@azure/core-xml` 1.6.0 (fast-xml-parser 5.11.2), in
  `test/node/node_modules`. JS paths below are relative to `@azure/service-bus/dist/esm/`.

How the facts were established. Most were checked empirically, not only by reading code: a
capture harness served canned ATOM XML and logged every request from both SDKs, and flipped
"fault modes" to see which response deviations break which SDK. All canned responses in this
document parse in both SDKs.

Facts marked **[captured]** were observed on the wire. Facts marked **[code]** come from reading
the source only.

---

## 0. Transport, base URL, auth

### 0.1 Scheme, host and port

| | Go | JS |
|---|---|---|
| Base URL | always `https://{host[:port]}/`. `entity_manager.go:272` `Host: fmt.Sprintf("https://%s/", ns)` | `http://{host[:port]}/` when the connection string has `UseDevelopmentEmulator`, otherwise `https://`. `serviceBusAtomManagementClient.js:92` (`useTls = false`) and `:1249` (`${this.useTls ? "https" : "http"}://${this.endpoint}/${path}`) |
| Host source | `url.Parse(Endpoint).Host`, so the **port is kept** (`internal/conn/conn.go:76-82`) | `Endpoint.match(".*://([^/]*)")[1]`, so the **port is kept** (`:86`) |
| Effect of `UseDevelopmentEmulator=true` | Only `TokenProvider.InsecureDisableTLS`, which is used only for AMQP (`internal/namespace.go:518` `getAMQPHostURI` → `amqp://`). **The admin client ignores it and still uses https.** | Disables TLS for admin. **[code]** The check is a truthiness test on the raw string, so `UseDevelopmentEmulator=false` also disables TLS (unverified). |
| Emulator endpoint validation | With Emulator=true, Endpoint must be `sb://host` or `sb://host:port` (`conn.go:102-110`) | none |

Consequences for the server:

- **Go needs TLS on the admin port.** **[captured]** Go tests can pass
  `admin.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: <*http.Client trusting the cert>}}`.
  The harness did this with `httptest.NewTLSServer(...).Client()`. Alternatively, a custom
  `policy.Transporter` could rewrite `req.URL.Scheme = "http"`; auth is a custom per-retry policy,
  not a bearer policy, so azcore does not refuse plain http (not tried).
- **JS needs plain HTTP** on the same host:port.
- If users reuse the AMQP connection string (`Endpoint=sb://localhost:5672;…;UseDevelopmentEmulator=true`)
  for the admin clients, then AMQP, HTTP and HTTPS all target the **same port**. Choose one:
  - multiplex on that port by sniffing the first bytes: `AMQP` header `0x41 0x4D 0x51 0x50`,
    TLS ClientHello `0x16`, otherwise an HTTP method; or
  - document a separate admin connection string with a different port. The official emulator
    does this: AMQP on 5672, management on 5300.
- **[captured]** Both SDKs speak HTTP/1.1. Go sends `Accept-Encoding: gzip`; JS sends `gzip,deflate`.
  The server may reply uncompressed.

### 0.2 api-version

- Both send `api-version=2024-05`. Go: `entity_manager.go:37` (`defaultAPIVersion`), set via
  `q.Set` at `:186`. JS: `constants.js:258` (`CURRENT_API_VERSION`), set first in `getUrl`.
- Go lets the caller override it with `azcore.ClientOptions.APIVersion`; JS uses `options.serviceVersion`.
- The server should accept any value. It must not reject unknown versions or return different shapes.

### 0.3 Authorization header (SAS; both SDKs, every request)

Go format (`internal/sas/sas.go:126-135`):
`SharedAccessSignature sr=<lower(url.QueryEscape(fullRequestURL))>&sig=<url.QueryEscape(base64(HMACSHA256(key, sr+"\n"+se)))>&se=<unix>&skn=<keyName>`

- The signed audience is the **full request URL including the query string**, e.g.
  `sr=https%3a%2f%2f127.0.0.1%3a34627%2fmyqueue%3fapi-version%3d2024-05`. **[captured]**
- Expiry is now + 2h (`internal/sbauth/token_provider.go:38`).

JS format (`util/sasServiceClientCredentials.js:39-44`):
`SharedAccessSignature sig=<encodeURIComponent(base64 HMAC)>&se=<unix>&skn=<keyName>&sr=<encodeURIComponent(url.toLowerCase()).toLowerCase()>`

- The parameter order differs from Go.
- Expiry is now + 5 min.

Server rules:

- Parse the `key=value` pairs in **any order**.
- For an emulator, do not validate.
- If you do validate, compute the HMAC over the **raw, still-encoded `sr` value** + `\n` + `se`.
  Both SDKs sign the encoded form.

### 0.4 Supplementary authorization headers (PUT only)

| Header | Go (`per_retry_auth_policy.go:22-40`) | JS (`serviceBusAtomManagementClient.js:1166-1184`) |
|---|---|---|
| `ServiceBusSupplementaryAuthorization` | Sent when `ForwardTo != nil`, on queue and subscription create/update. Token audience `sr` = the **ForwardTo value as given** (e.g. `sr=fwdqueue`). **[captured]** | Sent when `ForwardTo` is set. Token `sr` = namespace host:port (e.g. `sr=127.0.0.1%3A45383`). **[captured]** |
| `ServiceBusDlqSupplementaryAuthorization` | Sent when `ForwardDeadLetteredMessagesTo != nil` | Sent when `ForwardDeadLetteredMessagesTo` is set (same token) |

Both are SAS strings `SharedAccessSignature …`. The server should **ignore** them.

### 0.5 Other request headers

- **`Content-Type`** (PUT only): `application/atom+xml;type=entry;charset=utf-8` in both SDKs.
  Go: `entity_manager.go:190`. JS: `:1185`.
- **`If-Match: *`**: sent **only on update** in both SDKs, never on create.
  Go: `admin_client_queue.go:376`, `admin_client_topic.go:359`, `admin_client_subscription.go:388`,
  `admin_client_rules.go:329`. JS: `:1163`.
- **JS-only** headers: `x-ms-client-request-id`, `traceparent` (when tracing is on), `Content-Length`.
  User-Agent is `azsdk-js-azureservicebus/7.10.0 …`.
- **Go-only**: User-Agent `azsdk-go-azsbadmin/v1.11.0 …`.

---

## 1. Operation table

`{q}` = queue, `{t}` = topic, `{s}` = subscription, `{r}` = rule. Every request carries
`api-version=2024-05`.

### 1.1 Common rules (both SDKs)

- **Names are not escaped.** Names are concatenated raw into the path.
  - Go: `"/"+name`, `fmt.Sprintf("/%s/Subscriptions/%s", …)`.
  - JS: `topic + "/Subscriptions/" + sub`.
  - Queue and topic names may contain `/`, so the router must handle multi-segment names.
    For example, `/a/b` is entity `a/b` unless the path contains `/Subscriptions/`.
- **The path casing is fixed**: `$Resources/Queues`, `$Resources/Topics`, `/Subscriptions/`,
  `/Rules/`, `$namespaceinfo`.

### 1.2 Request lines, exactly as captured

| Operation | Go request line **[captured]** | JS request line **[captured]** |
|---|---|---|
| Namespace properties | `GET /$namespaceinfo?api-version=2024-05` | same |
| Create queue | `PUT /{q}?api-version=…` | same |
| Update queue | `PUT /{q}?…` + `If-Match: *` | same |
| Get queue / queue runtime props / queueExists | `GET /{q}?…` (Go has no exists helper) | `GET /{q}?…` |
| Delete queue | `DELETE /{q}?…` | same |
| List queues (+ runtime) | `GET /$Resources/Queues?%24top=2&api-version=…`, then `?%24skip=2&%24top=2&api-version=…` | `GET /$Resources/Queues?api-version=…&%24top=2`, then `…&%24skip=2&%24top=2` |
| Create/update/get/delete topic, topic runtime props, topicExists | `PUT/GET/DELETE /{t}?…` | same |
| List topics (+ runtime) | `GET /$Resources/Topics?…` (same pattern as queues) | same |
| Create/update/get/delete subscription, subscription runtime props, subscriptionExists | `PUT/GET/DELETE /{t}/Subscriptions/{s}?…` | same |
| List subscriptions (+ runtime) | `GET /{t}/Subscriptions?%24top=2&%3F=&api-version=…`: **no trailing slash, plus a junk empty `?` parameter**, because `baseFragment` already ends in `?` and the pager adds another (`admin_client_subscription.go:249,307`, `admin_client.go:195`) | `GET /{t}/Subscriptions/?api-version=…&%24top=2`, **with a trailing slash** (`:768`) |
| Create/update/get/delete rule, ruleExists | `PUT/GET/DELETE /{t}/Subscriptions/{s}/Rules/{r}?…` | same |
| Create rule **without a name** (Go only) | `PUT /{t}/Subscriptions/{s}/Rules/?…` with `<Name>$Default</Name>` in the body. The URL uses `putProps.Name` (empty) while the body uses `makeRuleNameForProperties` (`admin_client_rules.go:308-309,338`). | n/a (the JS name is mandatory) |
| List rules | `GET /{t}/Subscriptions/{s}/Rules/?…`, **with a trailing slash** (`admin_client_rules.go:245`) | `GET /{t}/Subscriptions/{s}/Rules/?…`, with a trailing slash (`:1024`) |

The router must therefore:

- accept both `Subscriptions` and `Subscriptions/`, and `Rules/`;
- ignore unknown query keys such as `?`;
- decode `%24skip`/`%24top`. Both SDKs percent-encode the `$`, and Go's `url.Values` does too.
  The `$` in the **path** stays literal.
- **[decision]** handle the Go no-name rule case. Recommended: treat a PUT on `…/Rules/` as a
  create of the rule named in the body's `<Name>`, and return an `<entry>`. Returning the rules
  feed (what a naive router does) makes Go fail with
  `expected element type <entry> but have <feed>` **[captured]**.

### 1.3 Paging

Query parameters: `$skip` (offset) and `$top` (page size). The server must honor both.

**Go** (`admin_client.go:190-236`, `entityPager.Fetcher`):

- Sends `$top` only if `MaxPageSize > 0`, and `$skip` only if `skip > 0`.
- After each page it sets `skip += len(entries)`.
- It stops when a page has **0 entries**, or when `0 < len < MaxPageSize`.
- With `MaxPageSize = 0` (the default), the condition `len < 0` is never true. Go therefore keeps
  requesting until it gets an **empty feed**. **[captured]** e.g. `…?api-version` (3 entries),
  then `…?%24skip=3` (0 entries), then stop.
- If the server ignores `$skip`, Go loops forever.
- Go never reads `<link rel="next">`.

**JS** (`listQueuesPage` etc.; `serviceBusAtomManagementClient.js:221`, `:1216-1226`, `getMarkerFromNextLinkUrl`):

- The first request has no `$skip`, because `Number(undefined)` is NaN and therefore falsy. It
  carries `$top` only if the caller set `maxPageSize`.
- The next page token is the `$skip` query value of the feed's `<link rel="next" href="…">`.
- **Quirk: the next link is only found if `<feed>` contains at least two `<link>` elements.**
  `util/atomXmlHelper.js:214-224` (`parseLinkInfo`) returns undefined unless `feed.link` is an
  array, and fast-xml-parser yields an object for a single element.
  - **[captured]** `MODE=onelink` (only a `next` link) made JS stop after page 1.
  - Emit `<link rel="self" …/>` **and** `<link rel="next" …/>` whenever more data remains.
  - Omit `next` on the last page.
- The `href` must be an absolute URL that `new URL()` accepts, otherwise JS throws
  `Unable to parse the '$skip' …` (`:1265-1276`). Its `$skip` value must be a non-negative
  integer string (`throwIfInvalidContinuationToken`, `:1631`).
- With no `maxPageSize`, JS sends no `$top`, so **the server decides the page size**. If it caps
  (Azure's default is 100), it must emit `next`; otherwise JS silently truncates.
  - Go is unaffected in either case.
  - Simplest: when `$top` is absent, return everything.

### 1.4 Status codes the server should return

| Case | Server should return | Go behaviour | JS behaviour |
|---|---|---|---|
| Create (PUT, no If-Match) of a new entity | **201** + `<entry>` of the created entity. Any 2xx works for both SDKs. | Any status < 400 is success (`entity_manager.go:201`). **Requires an entry body**: an empty body yields an unmarshal EOF error. | Any 2xx (`atomXmlHelper.js:126`). **Requires an entry body**, otherwise `buildX(undefined)` → `PARSE_ERROR`. |
| Update (PUT + `If-Match: *`) | **200** + `<entry>` | same | same |
| Create of an existing entity | **409** + `<Error>` | `*azcore.ResponseError`, `StatusCode == 409` **[captured]** | `RestError` with `code = "MessageEntityAlreadyExistsError"`, `statusCode = 409` **[captured]**. **Exceptions** (`atomXmlHelper.js:352-363`): the code is `ServiceError` if the method is DELETE, if it is a PUT with `If-Match: *`, or if Detail contains `subcode=40901`. Don't put `SubCode=40901` in a create-conflict Detail. |
| Update of a non-existent entity | **404** + `<Error>` (recommended) | error, 404 | `MessageEntityNotFoundError` |
| Get of a non-existent entity | **404** + `<Error>`, **or** 200 + the "Publicly Listed Services" empty feed (§3.6). Both work in both SDKs. | `GetX` returns `(nil, nil)` for either (`admin_client.go:243-255`, `mapATOMError`). **[captured]** | Throws `RestError`, `code = "MessageEntityNotFoundError"`. With the empty feed `statusCode = 200`, and the message is synthesized (`:1192-1206`). **[captured]** The `*Exists` helpers return `false` on **any** error (`:392`, `:670`, `:950`, `:1136`). |
| Delete success | **200**, empty body (or any valid XML) | Body is ignored | Body is parsed only if non-empty |
| Delete of a non-existent entity | **404** + `<Error>` | Returns `err` (a `ResponseError` with 404). **Not** mapped to nil. **[captured]** | `MessageEntityNotFoundError` **[captured]** |
| List with no items | 200 + `<feed>` with **no `<entry>`** | stops | returns `[]` |
| Bad request | 400 + `<Error>` | error | `code = "ServiceError"` |
| Unauthorized | 401 | error | `UnauthorizedRequestError` |

Additional mappings and rules:

- JS also maps 403 → `QuotaExceededError` (or `InvalidOperationError` if Detail contains
  `subcode=40301`), and 503 → `ServerBusyError`.
- JS turns any status not in `HttpResponseCodes` (`constants.js:294`) into code `ServiceError`.
  The unrecognised codes include 429 and 422.

#### Error body

Shape (`entity_manager.go:62-66` `ManagementError`; JS `atomXmlHelper.js:306-339`):

```xml
<Error><Code>409</Code><Detail>The messaging entity 'ns:Queue:q' already exists. To know more visit https://aka.ms/sbResourceMgrExceptions. TrackingId:…, SystemTracker:…, Timestamp:…</Detail></Error>
```

- JS uses `Error.Detail` as `err.message`. Go embeds the raw body in the `ResponseError` string.
- **The error body must be valid XML, or empty.** JS parses every non-empty response body
  *before* looking at the status (`atomXmlHelper.js:53-69`). A plain-text error body therefore
  surfaces as `PARSE_ERROR` "expected the service to return valid xml content", and the 404/409
  code mapping is lost.

---

## 2. Request bodies

### 2.1 Envelope

Go (`xml.Marshal`, no XML declaration). **[captured]**

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect">…</QueueDescription></content></entry>
```

JS (core-xml `stringifyXML`, `util/atomXmlHelper.js:100-117`). It adds an XML declaration and
`<updated>`, and declares `xmlns:i` on the description. **[captured]**

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?><entry xmlns="http://www.w3.org/2005/Atom"><updated>2026-10-29T20:04:55.366Z</updated><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">…</QueueDescription></content></entry>
```

Rules for the server's request parser:

- Use a real namespace-aware XML parser.
- Locate `entry/content/*[1]`. The description element's local name tells the type:
  `QueueDescription`, `TopicDescription`, `SubscriptionDescription`, or `RuleDescription`.
- Match child elements by **local name**. Both SDKs use the connect namespace as the default
  namespace, so the children are unprefixed.
- **Accept arbitrary element order.**
  - JS and Go order differ (e.g. queue `AuthorizationRules`/`Status`/`ForwardTo`, and the
    CorrelationFilter fields).
  - JS has *ordering comments* claiming that order matters to Azure
    (`queueResourceSerializer.js:14-19`). The emulator should not care.
- **Absent element ⇒ "not specified".**
  - Both SDKs omit nil or undefined fields. JS strips them in `sanitizeSerializableObject`
    (`atomXmlHelper.js:82-91`); Go uses `omitempty`.
  - Empty elements occur:
    - Go **always** emits `<AuthorizationRules></AuthorizationRules>` for queue and topic,
      even with no rules **[captured]**.
    - JS sends `<QueueDescription …/>` (self-closing) for `createQueue(name)` with no options
      **[captured]**.
    - Treat an empty element as "no value / empty list".
- Ignore unknown or read-only elements.
  - JS update round-trips what it got from GET, including `EntityAvailabilityStatus` and
    `EnableExpress` (both queue and topic), plus `Status`, `ForwardTo`, `UserMetadata` and
    `MaxMessageSizeInKilobytes` **[captured]**.
  - JS never echoes `CountDetails`, `MessageCount`, `SizeInBytes`, `CreatedAt` and similar fields;
    `buildQueueOptions` whitelists the fields it sends.
  - Go may send `CreatedAt` in rules (empty, so it is omitted).

#### Encodings

| Kind | Go | JS | Server must accept |
|---|---|---|---|
| Durations (`LockDuration`, `DefaultMessageTimeToLive`, `AutoDeleteOnIdle`, `DuplicateDetectionHistoryTimeWindow`) | the caller's string, passed through (`*string`) | the caller's string, passed through | ISO 8601 durations as strings: `PT45S`, `P1D`, `PT1H`, `P10675199DT2H48M5.4775807S` ("infinite" = TimeSpan.MaxValue). **Do not range-check** (the project goal). Store the string or parse it into a duration with ≥ 100 ns precision, and return it canonically. |
| Booleans | `true`/`false` | `"true"`/`"false"` (`getStringOrUndefined(bool)`) | `true`/`false`. JS reads any case (`utils.js:217`). |
| Integers | decimal | decimal string | decimal |
| Status | `Active`, `Disabled`, `SendDisabled`, `ReceiveDisabled` | same | — |
| Text | XML-escaped (`meta &lt;&amp;&gt;`) | same | — |

#### XSI type attributes (rules, authorization rules)

Both SDKs declare the `http://www.w3.org/2001/XMLSchema-instance` prefix themselves, each with a
different name, so **resolve the attribute by namespace URI, not by prefix**:

- Go (encoding/xml auto-prefix) **[captured]**:
  `<Filter xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="SqlFilter">`.
- JS: `<Filter p4:type="SqlFilter" xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">`.
  For AuthorizationRule it uses `p5:type="SharedAccessAuthorizationRule"`.
- Value elements carry a QName type `l28:string` etc. with `xmlns:l28="http://www.w3.org/2001/XMLSchema"`
  declared **on the Value element itself**. Their type attribute uses the prefix declared on an
  ancestor: `_XMLSchema-instance:type` (Go) or `p4:type` (JS). Take the local part after `:`.

### 2.2 QueueDescription

Element order as sent.

| # | Go (`models.go:60-89`, struct order) | JS (`queueResourceSerializer.js:12-39`) |
|---|---|---|
| 1 | LockDuration | LockDuration |
| 2 | MaxSizeInMegabytes | MaxSizeInMegabytes |
| 3 | RequiresDuplicateDetection | RequiresDuplicateDetection |
| 4 | RequiresSession | RequiresSession |
| 5 | DefaultMessageTimeToLive | DefaultMessageTimeToLive |
| 6 | DeadLetteringOnMessageExpiration | DeadLetteringOnMessageExpiration |
| 7 | DuplicateDetectionHistoryTimeWindow | DuplicateDetectionHistoryTimeWindow |
| 8 | MaxDeliveryCount | MaxDeliveryCount |
| 9 | EnableBatchedOperations | EnableBatchedOperations |
| 10 | AuthorizationRules (**always present**) | AuthorizationRules |
| 11 | Status | Status |
| 12 | AutoDeleteOnIdle | ForwardTo |
| 13 | EnablePartitioning | UserMetadata |
| 14 | ForwardTo | AutoDeleteOnIdle |
| 15 | ForwardDeadLetteredMessagesTo | EnablePartitioning |
| 16 | UserMetadata | ForwardDeadLetteredMessagesTo |
| 17 | MaxMessageSizeInKilobytes | EntityAvailabilityStatus (update round-trip) |
| 18 | | EnableExpress |
| 19 | | MaxMessageSizeInKilobytes |

Go cannot send `EnableExpress`, `SupportOrdering`, `RequiresSession` on update only, etc. It
only sends the fields that `newQueueEnvelope` maps (`admin_client_queue.go:403-425`).

Captured Go create, all properties set:

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><LockDuration>PT45S</LockDuration><MaxSizeInMegabytes>2048</MaxSizeInMegabytes><RequiresDuplicateDetection>true</RequiresDuplicateDetection><RequiresSession>true</RequiresSession><DefaultMessageTimeToLive>P1D</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DuplicateDetectionHistoryTimeWindow>PT5M</DuplicateDetectionHistoryTimeWindow><MaxDeliveryCount>7</MaxDeliveryCount><EnableBatchedOperations>false</EnableBatchedOperations><AuthorizationRules><AuthorizationRule xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="SharedAccessAuthorizationRule"><ClaimType>SharedAccessKey</ClaimType><ClaimValue>None</ClaimValue><Rights><AccessRights>Listen</AccessRights><AccessRights>Send</AccessRights></Rights><KeyName>k1</KeyName><PrimaryKey>pk</PrimaryKey><SecondaryKey>sk</SecondaryKey></AuthorizationRule></AuthorizationRules><Status>Active</Status><AutoDeleteOnIdle>PT1H</AutoDeleteOnIdle><EnablePartitioning>false</EnablePartitioning><ForwardTo>fwdqueue</ForwardTo><ForwardDeadLetteredMessagesTo>dlqfwd</ForwardDeadLetteredMessagesTo><UserMetadata>meta &lt;&amp;&gt;</UserMetadata><MaxMessageSizeInKilobytes>1024</MaxMessageSizeInKilobytes></QueueDescription></content></entry>
```

Captured JS create, all properties set. Note that `forwardTo: "fwdqueue"` was rewritten:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?><entry xmlns="http://www.w3.org/2005/Atom"><updated>2026-10-29T20:04:55.366Z</updated><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><LockDuration>PT45S</LockDuration><MaxSizeInMegabytes>2048</MaxSizeInMegabytes><RequiresDuplicateDetection>true</RequiresDuplicateDetection><RequiresSession>true</RequiresSession><DefaultMessageTimeToLive>P1D</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DuplicateDetectionHistoryTimeWindow>PT5M</DuplicateDetectionHistoryTimeWindow><MaxDeliveryCount>7</MaxDeliveryCount><EnableBatchedOperations>false</EnableBatchedOperations><AuthorizationRules><AuthorizationRule p5:type="SharedAccessAuthorizationRule" xmlns:p5="http://www.w3.org/2001/XMLSchema-instance"><ClaimType>SharedAccessKey</ClaimType><ClaimValue>None</ClaimValue><Rights><AccessRights>Listen</AccessRights><AccessRights>Send</AccessRights></Rights><KeyName>k1</KeyName><PrimaryKey>pk</PrimaryKey><SecondaryKey>sk</SecondaryKey></AuthorizationRule></AuthorizationRules><Status>Active</Status><ForwardTo>sb://127.0.0.1:45383/fwdqueue</ForwardTo><UserMetadata>meta &lt;&amp;&gt;</UserMetadata><AutoDeleteOnIdle>PT1H</AutoDeleteOnIdle><EnablePartitioning>false</EnablePartitioning><ForwardDeadLetteredMessagesTo>sb://127.0.0.1:45383/dlqfwd</ForwardDeadLetteredMessagesTo><MaxMessageSizeInKilobytes>1024</MaxMessageSizeInKilobytes></QueueDescription></content></entry>
```

Captured Go update with only `LockDuration` set:

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><LockDuration>PT2M</LockDuration><AuthorizationRules></AuthorizationRules></QueueDescription></content></entry>
```

**Update semantics [decision].** The two SDKs send different update bodies:

- Go `Update*` sends only the fields the caller set in the struct. Its docs and tests
  Get → modify → Update, so in practice the full set is sent.
- JS sends the full object.

Merge (absent element = keep the current value) and replace (absent = default) are therefore
observably different only for Go callers who pass a sparse struct. The SDK code doesn't settle
which one Azure does. Pick one and document it; **merge** is the friendlier choice for an emulator.

#### ForwardTo / ForwardDeadLetteredMessagesTo

- **Go sends the value verbatim.** Go's own live test sends `sb://{endpoint}/{queue}` and asserts
  that GET returns exactly that string (`admin/admin_client_test.go:724-738`). Go docs call it
  "the name of the recipient entity", so users may also send a bare name.
- **JS rewrites a non-absolute value** to `"sb://" + host[:port] + "/" + value`.
  `isAbsoluteUrl` checks for an `sb://`, `http://` or `https://` prefix (`utils.js:379-382`;
  `serviceBusAtomManagementClient.js:1173-1183`).
- Server:
  - accept either form;
  - resolve the target entity by stripping `scheme://host[:port]/`;
  - **return exactly the string that was stored**, which satisfies the Go test and JS round-trips;
  - accept an empty value as "clear".

#### AuthorizationRules

- Accept `AuthorizationRule` with any xsi:type prefix and the children `ClaimType`,
  `ClaimValue`, `Rights/AccessRights*`, `KeyName`, `PrimaryKey`, `SecondaryKey`, and optionally
  `CreatedTime`/`ModifiedTime`.
- Go sends `ClaimType=SharedAccessKey` and `ClaimValue=None` (`admin_client_models.go:84-101`).

### 2.3 TopicDescription

| Go order (`models.go:125-152`) | JS order (`topicResourceSerializer.js:12-35`) |
|---|---|
| DefaultMessageTimeToLive | DefaultMessageTimeToLive |
| MaxSizeInMegabytes | MaxSizeInMegabytes |
| RequiresDuplicateDetection | RequiresDuplicateDetection |
| DuplicateDetectionHistoryTimeWindow | DuplicateDetectionHistoryTimeWindow |
| EnableBatchedOperations | EnableBatchedOperations |
| AuthorizationRules (always present) | AuthorizationRules |
| Status | Status |
| UserMetadata | UserMetadata |
| SupportOrdering | SupportOrdering |
| AutoDeleteOnIdle | AutoDeleteOnIdle |
| EnablePartitioning | EnablePartitioning |
| MaxMessageSizeInKilobytes | EntityAvailabilityStatus |
| | EnableExpress |
| | MaxMessageSizeInKilobytes |

Captured Go create:

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><TopicDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><DefaultMessageTimeToLive>PT10M</DefaultMessageTimeToLive><MaxSizeInMegabytes>1024</MaxSizeInMegabytes><RequiresDuplicateDetection>true</RequiresDuplicateDetection><DuplicateDetectionHistoryTimeWindow>PT1M</DuplicateDetectionHistoryTimeWindow><EnableBatchedOperations>true</EnableBatchedOperations><AuthorizationRules></AuthorizationRules><Status>Active</Status><UserMetadata>tm</UserMetadata><SupportOrdering>true</SupportOrdering><AutoDeleteOnIdle>P10675199DT2H48M5.4775807S</AutoDeleteOnIdle><EnablePartitioning>false</EnablePartitioning><MaxMessageSizeInKilobytes>256</MaxMessageSizeInKilobytes></TopicDescription></content></entry>
```

### 2.4 SubscriptionDescription

| Go order (`models.go:266-287`) | JS order (`subscriptionResourceSerializer.js:13-38`) |
|---|---|
| LockDuration | LockDuration |
| RequiresSession | RequiresSession |
| DefaultMessageTimeToLive | DefaultMessageTimeToLive |
| DeadLetteringOnMessageExpiration | DeadLetteringOnMessageExpiration |
| DeadLetteringOnFilterEvaluationExceptions | DeadLetteringOnFilterEvaluationExceptions |
| DefaultRuleDescription | DefaultRuleDescription |
| MaxDeliveryCount | MaxDeliveryCount |
| EnableBatchedOperations | EnableBatchedOperations |
| Status | Status |
| ForwardTo | ForwardTo |
| UserMetadata | UserMetadata |
| ForwardDeadLetteredMessagesTo | ForwardDeadLetteredMessagesTo |
| AutoDeleteOnIdle | AutoDeleteOnIdle |
| | EntityAvailabilityStatus |

- Go `newSubscriptionEnvelope` (`admin_client_subscription.go:414-437`) does **not** send `Status`,
  even though the public struct has it.
- Captured Go updates sent only `<MaxDeliveryCount>4</MaxDeliveryCount>`.

#### DefaultRuleDescription

DefaultRuleDescription is used **only on create**.

- If it is present, the subscription's initial rule is this filter/action under its `<Name>`,
  and there is **no** separate `$Default` TrueFilter.
- If it is absent, create a `$Default` rule with TrueFilter `1=1` and EmptyRuleAction.
- Name defaults:
  - Go always fills `<Name>`; it defaults to `$Default` (`admin_client_rules.go:349-355`).
  - JS omits `<Name>` if `defaultRuleOptions.name` is undefined, so default to `$Default`.
- Go may omit `<Action>` (`models.go:173-178`, `Action omitempty`). Treat that as EmptyRuleAction.
- Inside DefaultRuleDescription the namespace is inherited (connect). Its Filter and Action follow
  the same format as in §2.5.

Captured Go subscription create with an SQL default rule:

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><LockDuration>PT30S</LockDuration><RequiresSession>false</RequiresSession><DefaultMessageTimeToLive>PT1H</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>true</DeadLetteringOnFilterEvaluationExceptions><DefaultRuleDescription><Filter xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="SqlFilter"><SqlExpression>color = @c</SqlExpression><Parameters><KeyValueOfstringanyType><Key>@c</Key><Value _XMLSchema-instance:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">blue</Value></KeyValueOfstringanyType></Parameters></Filter><Action xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="SqlRuleAction"><SqlExpression>SET x = 1</SqlExpression></Action><Name>myrule</Name></DefaultRuleDescription><MaxDeliveryCount>3</MaxDeliveryCount><EnableBatchedOperations>true</EnableBatchedOperations><ForwardTo>fwdq</ForwardTo><UserMetadata>sm</UserMetadata><AutoDeleteOnIdle>PT2H</AutoDeleteOnIdle></SubscriptionDescription></content></entry>
```

Captured Go create with a CorrelationFilter default rule, no name and no action:

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><DefaultRuleDescription><Filter xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="CorrelationFilter"><CorrelationId>cid</CorrelationId></Filter><Name>$Default</Name></DefaultRuleDescription></SubscriptionDescription></content></entry>
```

Captured JS subscription create with an SQL default rule:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?><entry xmlns="http://www.w3.org/2005/Atom"><updated>2026-10-29T20:04:55.427Z</updated><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><LockDuration>PT30S</LockDuration><RequiresSession>false</RequiresSession><DefaultMessageTimeToLive>PT1H</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>true</DeadLetteringOnFilterEvaluationExceptions><DefaultRuleDescription><Filter p4:type="SqlFilter" xmlns:p4="http://www.w3.org/2001/XMLSchema-instance"><SqlExpression>color = @c</SqlExpression><Parameters><KeyValueOfstringanyType><Key>@c</Key><Value p4:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">blue</Value></KeyValueOfstringanyType></Parameters></Filter><Action p4:type="SqlRuleAction" xmlns:p4="http://www.w3.org/2001/XMLSchema-instance"><SqlExpression>SET x = 1</SqlExpression></Action><Name>myrule</Name></DefaultRuleDescription><MaxDeliveryCount>3</MaxDeliveryCount><EnableBatchedOperations>true</EnableBatchedOperations><Status>Active</Status><ForwardTo>sb://127.0.0.1:45383/fwdq</ForwardTo><UserMetadata>sm</UserMetadata><AutoDeleteOnIdle>PT2H</AutoDeleteOnIdle></SubscriptionDescription></content></entry>
```

### 2.5 RuleDescription (create/update rule)

#### Element order

| Go (`models.go:162-171`; `xmlns` and `xmlns:i` declared on RuleDescription) | JS (`ruleResourceSerializer.js:61-127`) |
|---|---|
| (`CreatedAt`, omitted when empty), `Filter`, `Action` (**omitted when there is no action**), `Name` | `Filter`, `Action` (**always present**: EmptyRuleAction if there is no sqlExpression), `Name` |

#### Filter xsi:type values sent

| Filter | Go | JS |
|---|---|---|
| SQL | `SqlFilter` + `SqlExpression` + optional `Parameters` | `SqlFilter` + `SqlExpression` + optional `Parameters` |
| Correlation | `CorrelationFilter` | `CorrelationFilter` |
| True | `TrueFilter` with `<SqlExpression>1=1</SqlExpression>` (also the Go default when Filter is nil, `admin_client_rules.go:357-363`) | no TrueFilter type; sends `SqlFilter` with `1=1` (the default when filter is undefined) |
| False | `FalseFilter` with `<SqlExpression>1=0</SqlExpression>` | none (`SqlFilter` `1=0` if the user writes it) |
| Unknown | Go `UnknownRuleFilter` re-sends raw XML with `i:type` | — |

#### Action xsi:type values sent

| Action | Go | JS |
|---|---|---|
| SQL | `SqlRuleAction` + `SqlExpression` + optional `Parameters` | `SqlRuleAction` + `SqlExpression` + optional `Parameters` |
| Empty | no element | `EmptyRuleAction`, self-closing |

#### CompatibilityLevel

**Never sent by either SDK.** Go's `int` field has `omitempty` (`models.go:202`).

#### CorrelationFilter child order

- Go: `CorrelationId, MessageId, To, ReplyTo, Label, SessionId, ReplyToSessionId, ContentType, Properties` (`models.go:338-348`).
- JS: `CorrelationId, Label, To, ReplyTo, ReplyToSessionId, ContentType, SessionId, MessageId, Properties` (`ruleResourceSerializer.js:91-101`).
- `Label` is the AMQP **subject**.

#### Properties / Parameters: `KeyValueOfstringanyType`

The shape is `<KeyValueOfstringanyType><Key>k</Key><Value {xsi}:type="l28:T" xmlns:l28="http://www.w3.org/2001/XMLSchema">v</Value></KeyValueOfstringanyType>`.

| Value kind | Go (`admin_client_rules.go:534-595`) | JS (`ruleResourceSerializer.js:232-277`) |
|---|---|---|
| string | `l28:string` | `l28:string` |
| integer | `l28:int`, `%d` (int, int32, int64) | **`l28:double`** for *every* JS number, e.g. `7` |
| float | `l28:double`, `%f` (e.g. `1.500000`) | `l28:double`, JS `String(n)` |
| bool | `l28:boolean`, `true`/`false` | `l28:boolean` |
| time | `l28:dateTime`, RFC 3339 Nano UTC (`2024-01-02T03:04:05Z`) | `l28:dateTime`, `Date.toJSON()` (`2024-01-02T03:04:05.000Z`) |
| other | error (no duration/timespan support) | TypeError |

JS also defines `l28:long` (`ruleResourceSerializer.js:156`) but never emits it.

The server's message matching must compare a correlation property sent as `double 7` (JS) or
`int 7` (Go) against an AMQP application property that may be int, long or double. Compare
numerically.

#### Captured requests

Go, SqlFilter + SqlRuleAction with every parameter type:

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><Filter xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="SqlFilter"><SqlExpression>color = @c AND n &gt; @n</SqlExpression><Parameters><KeyValueOfstringanyType><Key>@c</Key><Value _XMLSchema-instance:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">blue</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>@n</Key><Value _XMLSchema-instance:type="l28:int" xmlns:l28="http://www.w3.org/2001/XMLSchema">5</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>@b</Key><Value _XMLSchema-instance:type="l28:boolean" xmlns:l28="http://www.w3.org/2001/XMLSchema">true</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>@f</Key><Value _XMLSchema-instance:type="l28:double" xmlns:l28="http://www.w3.org/2001/XMLSchema">1.500000</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>@t</Key><Value _XMLSchema-instance:type="l28:dateTime" xmlns:l28="http://www.w3.org/2001/XMLSchema">2024-01-02T03:04:05Z</Value></KeyValueOfstringanyType></Parameters></Filter><Action xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="SqlRuleAction"><SqlExpression>SET x = @v</SqlExpression><Parameters><KeyValueOfstringanyType><Key>@v</Key><Value _XMLSchema-instance:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">y</Value></KeyValueOfstringanyType></Parameters></Action><Name>sqlrule</Name></RuleDescription></content></entry>
```

Go, CorrelationFilter with every field and one property (no Action element):

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><Filter xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="CorrelationFilter"><CorrelationId>cid</CorrelationId><MessageId>mid</MessageId><To>to</To><ReplyTo>rt</ReplyTo><Label>subj</Label><SessionId>sid</SessionId><ReplyToSessionId>rtsid</ReplyToSessionId><ContentType>ct</ContentType><Properties><KeyValueOfstringanyType><Key>color</Key><Value _XMLSchema-instance:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">red</Value></KeyValueOfstringanyType></Properties></Filter><Name>corrrule</Name></RuleDescription></content></entry>
```

Go, TrueFilter / FalseFilter:

```xml
<RuleDescription xmlns="…/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><Filter xmlns:_XMLSchema-instance="http://www.w3.org/2001/XMLSchema-instance" _XMLSchema-instance:type="TrueFilter"><SqlExpression>1=1</SqlExpression></Filter><Name>truerule</Name></RuleDescription>
<RuleDescription …><Filter … _XMLSchema-instance:type="FalseFilter"><SqlExpression>1=0</SqlExpression></Filter><Name>falserule</Name></RuleDescription>
```

JS, SqlFilter + SqlRuleAction:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?><entry xmlns="http://www.w3.org/2005/Atom"><updated>2026-10-29T20:04:55.448Z</updated><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><Filter p4:type="SqlFilter" xmlns:p4="http://www.w3.org/2001/XMLSchema-instance"><SqlExpression>color = @c AND n &gt; @n</SqlExpression><Parameters><KeyValueOfstringanyType><Key>@c</Key><Value p4:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">blue</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>@n</Key><Value p4:type="l28:double" xmlns:l28="http://www.w3.org/2001/XMLSchema">5</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>@b</Key><Value p4:type="l28:boolean" xmlns:l28="http://www.w3.org/2001/XMLSchema">true</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>@t</Key><Value p4:type="l28:dateTime" xmlns:l28="http://www.w3.org/2001/XMLSchema">2024-01-02T03:04:05.000Z</Value></KeyValueOfstringanyType></Parameters></Filter><Action p4:type="SqlRuleAction" xmlns:p4="http://www.w3.org/2001/XMLSchema-instance"><SqlExpression>SET x = @v</SqlExpression><Parameters><KeyValueOfstringanyType><Key>@v</Key><Value p4:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">y</Value></KeyValueOfstringanyType></Parameters></Action><Name>sqlrule</Name></RuleDescription></content></entry>
```

JS, CorrelationFilter with every field and two properties (one numeric):

```xml
…<RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><Filter p4:type="CorrelationFilter" xmlns:p4="http://www.w3.org/2001/XMLSchema-instance"><CorrelationId>cid</CorrelationId><Label>subj</Label><To>to</To><ReplyTo>rt</ReplyTo><ReplyToSessionId>rtsid</ReplyToSessionId><ContentType>ct</ContentType><SessionId>sid</SessionId><MessageId>mid</MessageId><Properties><KeyValueOfstringanyType><Key>color</Key><Value p4:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">red</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>n</Key><Value p4:type="l28:double" xmlns:l28="http://www.w3.org/2001/XMLSchema">7</Value></KeyValueOfstringanyType></Properties></Filter><Action p4:type="EmptyRuleAction" xmlns:p4="http://www.w3.org/2001/XMLSchema-instance"/><Name>corrrule</Name></RuleDescription>…
```

#### Rule server rules

- **The name in the URL is authoritative**, except in the Go empty-URL-name case (§1.2).
- Go `UpdateRule` sends the full `RuleProperties`. JS `updateRule` sends the full rule from GET.
- Treat a missing `Action` as `EmptyRuleAction`, and a missing `Filter` as TrueFilter.
- Accept and store `TrueFilter`/`FalseFilter` as their own types. They may arrive with a
  `SqlExpression`, which can be ignored.

---

## 3. Response bodies

### 3.1 How each SDK parses responses

**Go: encoding/xml.**

- Elements match by **local name only**, so prefixes and namespaces on elements don't matter.
- Typed fields fail on malformed values: `*int32` on a non-number gives an unmarshal error, and
  `*bool` accepts `true`/`false`/`1`/`0`.
- Attributes declared with a namespace URI **do** require the namespace: the `xsi:type` on
  `Filter`, `Action`, `Value` and `AuthorizationRule`. Their prefix must be bound to
  `http://www.w3.org/2001/XMLSchema-instance` in scope, e.g. `xmlns:i` on the description element.
- The entity name comes from `<title>` for list items:
  - `newQueueItem` → `env.Title` (`admin_client_queue.go:450-453`);
  - topics → `admin_client_topic.go:404`;
  - subscriptions → `admin_client_subscription.go:488`.
  For single Gets, Go uses the name the caller passed.
- Rules use `<RuleDescription><Name>` (`admin_client_rules.go:456-458`).
- Go never reads `<id>`.

**JS: fast-xml-parser.** Options are in `@azure/core-xml/dist/esm/xml.js:5-34`:
`attributesGroupName: "$"`, `textNodeName: "_"`, `ignoreAttributes: false`,
`parseTagValue: false` (all values are strings), `attributeNamePrefix: ""`, `trimValues: false`,
and `includeRoot: true` from `atomXmlHelper.js:55`.

- **Prefixes are kept literally in keys.** An element `d2p1:ActiveMessageCount` is the key
  `"d2p1:ActiveMessageCount"`, and an attribute `i:type` is `$["i:type"]`. JS therefore depends on
  specific prefixes (§3.4, §3.5).
- Elements of the descriptions must be **unprefixed** (default namespace), and so must the atom
  elements (`feed`, `entry`, `content`, `id`, `link`).
- **[captured]** Pretty-printed responses, with whitespace between elements, parse fine in both
  SDKs (`MODE=ws`).
- **[captured]** An `<?xml …?>` declaration is fine (`MODE=xmldecl`).
- The response is accepted if the root is `feed` or `entry` (`atomXmlHelper.js:140-172`):
  - **feed**: each `entry` is parsed, and the feed's `link` array gives `nextLink`;
  - **entry**: `entry.content` must be an object. The **first non-`$` key** of `content` is the
    description, so put nothing else in `<content>`. The other entry children (`id`, `title`, …)
    are copied to a metadata object.
  - A bare `<feed/>` parses to `""` and throws `PARSE_ERROR`. Always give the feed children.
- **Names come from `<id>`.** `setName` (`atomXmlHelper.js:271-300`) parses the **absolute URL**
  in `<entry><id>` and derives the name from its path:
  - `…/{topic}/Subscriptions/{sub}/Rules/{rule}` → TopicName, SubscriptionName, RuleName;
  - `…/{topic}/Subscriptions/{sub}` → TopicName, SubscriptionName;
  - `…/{name}` → QueueName/TopicName.
  - A leading `sb://` is rewritten to `https://`.
  - **[captured]** A missing `<id>` gives an uncaught `TypeError: Cannot read properties of undefined (reading 'startsWith')` (`MODE=noid`).
  - **[captured]** A relative `<id>` gives `TypeError: Invalid URL` (`MODE=relid`).
  - The pathname is **not** URL-decoded, so names come back exactly as they appear in the id.
  - The id must be plain text with no attributes.
- **JS never reads `<title>`, and never reads the rule's `<Name>`.**

### 3.2 Entry template (single entity; also the PUT 200/201 response)

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><id>https://{host}/{path}?api-version=2024-05</id><title type="text">{lastName}</title><published>{created}</published><updated>{updated}</updated><author><name>{namespace}</name></author><link rel="self" href="https://{host}/{path}?api-version=2024-05"/><content type="application/xml">{Description}</content></entry>
```

- `{path}` is `q`, `t`, `t/Subscriptions/s`, or `t/Subscriptions/s/Rules/r`.
- `<title>` must be the **last segment** (the entity name), because Go reads it.
- The `<id>` scheme/host is irrelevant to the SDKs; use `https://{Host header}/`.
- Use response Content-Type `application/atom+xml;type=entry;charset=utf-8` (or `type=feed` for
  feeds). Neither SDK checks it.

### 3.3 Fields each SDK requires in responses

**R** = the SDK throws or panics if the field is missing. **o** = optional (nil/undefined/0).
JS getters (`utils.js`):

- `getString`, `getInteger` and `getBoolean` throw on missing. All JS throws are wrapped as
  `RestError` `PARSE_ERROR` "cannot form a X object".
- `getBoolean` is `toLowerCase()==="true"`, so any non-"true" value is false.
- `getDate` is `new Date(str)`.

#### QueueDescription

| Element | JS getQueue / list (`buildQueue`, `queueResourceSerializer.js:46-69`) | JS runtime (`:75-86`) | Go |
|---|---|---|---|
| LockDuration | R | — | o |
| MaxSizeInMegabytes | R (int) | — | o |
| MaxDeliveryCount | R (int) | — | o |
| EnablePartitioning | R (bool) | — | o |
| RequiresSession | R | — | o |
| EnableBatchedOperations | R | — | o |
| DefaultMessageTimeToLive | R | — | o |
| RequiresDuplicateDetection | R | — | o |
| DuplicateDetectionHistoryTimeWindow | R | — | o |
| DeadLetteringOnMessageExpiration | R | — | o |
| **EnableExpress** | **R** (verified with `MODE=noexpress` → PARSE_ERROR) | — | o |
| ForwardTo, ForwardDeadLetteredMessagesTo, UserMetadata, AutoDeleteOnIdle, Status, EntityAvailabilityStatus, MaxMessageSizeInKilobytes, AuthorizationRules | o | — | o |
| SizeInBytes, MessageCount | — | o | o |
| CountDetails | — | **R**, see §3.4 | **R** for runtime props (`admin_client_queue.go:459-461`), children optional |
| CreatedAt, UpdatedAt, AccessedAt | — | **R** (`getDate` → `getString`) | Runtime props parse them with `time.RFC3339`. Missing or empty causes an **error** (empty string fails `time.Parse`). Special case: `0001-01-01T00:00:00` with no Z (`models.go:390-406`). |

#### TopicDescription

| Element | JS getTopic (`buildTopic`, `topicResourceSerializer.js:41-59`) | JS runtime (`:65-78`) | Go |
|---|---|---|---|
| MaxSizeInMegabytes | R | — | o |
| EnablePartitioning | R | — | o |
| **SupportOrdering** | **R** | — | o |
| EnableBatchedOperations | R | — | o |
| DefaultMessageTimeToLive | R | — | o |
| RequiresDuplicateDetection | R | — | o |
| DuplicateDetectionHistoryTimeWindow | R | — | o |
| **EnableExpress** | **R** | — | o |
| AutoDeleteOnIdle, AuthorizationRules, UserMetadata, Status, EntityAvailabilityStatus, MaxMessageSizeInKilobytes | o | — | o |
| SizeInBytes, SubscriptionCount, SqlFilterCount, CorrelationFilterCount | — | o | o |
| CountDetails | — | **R** (only ScheduledMessageCount is used) | **R** (`admin_client_topic.go:425-427`) |
| CreatedAt, UpdatedAt, AccessedAt | — | R | R (runtime) |

#### SubscriptionDescription

| Element | JS getSubscription (`buildSubscription`, `subscriptionResourceSerializer.js:44-62`) | JS runtime (`:68-82`) | Go |
|---|---|---|---|
| LockDuration | R | — | o |
| MaxDeliveryCount | R | — | o |
| RequiresSession | R | — | o |
| EnableBatchedOperations | R | — | o |
| DefaultMessageTimeToLive | R | — | o |
| **AutoDeleteOnIdle** | **R** | — | o |
| DeadLetteringOnMessageExpiration | R | — | o |
| **DeadLetteringOnFilterEvaluationExceptions** | **R** | — | o |
| **Status** | **R** | — | o |
| **EntityAvailabilityStatus** | **R** (`MODE=noavail` → PARSE_ERROR **[captured]**) | — | o |
| ForwardTo, ForwardDeadLetteredMessagesTo, UserMetadata | o | — | o |
| MessageCount | — | **R** (int) | **R**: dereferenced without a nil check, so missing **panics** (`admin_client_subscription.go:501`) |
| CountDetails | — | R | **R**. Active, DeadLetter, Transfer and TransferDeadLetter are all **dereferenced (panic if missing)** (`:502-505`). |
| CreatedAt, UpdatedAt, AccessedAt | — | R | R |

#### NamespaceInfo

The entry is `<content><NamespaceInfo …>` (Go reads `content>NamespaceInfo`, `models.go:377-387`).

| Element | JS (`namespaceResourceSerializer.js:10-21`) | Go (`admin_client.go:108-132`) |
|---|---|---|
| CreatedTime | R (date) | R (RFC 3339) |
| ModifiedTime | R | R |
| Name | R | o |
| MessagingSKU | R | o |
| MessagingUnits | R **only if** the SKU is `Premium` | o |
| NamespaceType | ignored | ignored |

The entry still needs an absolute `<id>`, because JS calls `setName` on it.

### 3.4 CountDetails (namespace prefix)

JS `getMessageCountDetails` / `getXMLNSPrefix` (`utils.js:248-283`) requires:

- **exactly one attribute** on `<CountDetails>`;
- that the attribute is an `xmlns:{prefix}` declaration;
- that every child uses that prefix.

`getXMLNSPrefix` runs **before** the null check, so a missing CountDetails throws. A default
`xmlns="…"` instead of a prefix throws too (`MODE=cddefault` → PARSE_ERROR **[captured]**). Go
doesn't care about prefixes.

Use exactly the Azure form:

```xml
<CountDetails xmlns:d2p1="http://schemas.microsoft.com/netservices/2011/06/servicebus"><d2p1:ActiveMessageCount>3</d2p1:ActiveMessageCount><d2p1:DeadLetterMessageCount>1</d2p1:DeadLetterMessageCount><d2p1:ScheduledMessageCount>2</d2p1:ScheduledMessageCount><d2p1:TransferMessageCount>0</d2p1:TransferMessageCount><d2p1:TransferDeadLetterMessageCount>0</d2p1:TransferDeadLetterMessageCount></CountDetails>
```

Emit all five children for queues, topics and subscriptions; Go subscription parsing panics if
any of the four non-scheduled children are missing.

### 3.5 RuleDescription response

Requirements:

- **`<Filter>` and `<Action>` are both mandatory.**
  - Go dereferences `desc.Filter.Type` and `desc.Action.Type`. **[captured]** A missing Action
    **panics** (`admin_client_rules.go:508`, nil pointer; `MODE=noaction`).
  - JS `getRuleAction(undefined)` throws, giving `PARSE_ERROR`.
  - With no action, emit `<Action i:type="EmptyRuleAction"/>`.
- **Go filter type** comes from the namespaced `type` attribute: `TrueFilter`, `FalseFilter`,
  `CorrelationFilter`, or `SqlFilter`. Anything else becomes `UnknownRuleFilter`.
  - **SqlFilter must contain `<SqlExpression>`**; Go dereferences it (`:493`).
  - Action types are `EmptyRuleAction`, `SqlRuleAction`, or other → `UnknownRuleAction`.
- **JS filter type** is decided **only by the presence of `<SqlExpression>`**
  (`ruleResourceSerializer.js:26`):
  - Correlation filters must **not** contain `SqlExpression`.
  - TrueFilter/FalseFilter **must** contain `<SqlExpression>1=1</SqlExpression>` /
    `<SqlExpression>1=0</SqlExpression>`. Otherwise JS reports an empty correlation filter.
- **KeyValue values in responses (JS quirk).** JS reads `Value["$"]["i:type"]`
  (`ruleResourceSerializer.js:203`), so:
  - **the attribute must be literally `i:type`**;
  - JS takes `.substring(5)`, so **the type prefix must be exactly 4 characters**. Use
    `d6p1:string` like Azure. **[captured]** With `l28:string`, JS throws PARSE_ERROR
    (`MODE=badprefix`); Go is fine either way, since it splits on `:`.
  - JS reads the text through `Value["_"]`.
  - Allowed type names:
    - JS accepts `int`, `double`, `string`, `boolean`, `dateTime` (`:163-170, 204-219`);
    - Go accepts `string`, `boolean`, `int`, `double`, `dateTime` (`admin_client_rules.go:614-652`);
    - **never emit `long`** (or anything else); both SDKs reject it.
  - Map stored numeric types: Go int → `int`, JS double → `double`.
  - For Go, `xmlns:i` must be in scope (declare it on RuleDescription) and
    `xmlns:d6p1="http://www.w3.org/2001/XMLSchema"` must be on Value.
- **Name.** Go reads the name from `<Name>`. JS reads it from the `<id>` URL
  (`/Rules/{name}`). Emit both.
- `CompatibilityLevel` is not read by either SDK. Emit `20` like Azure (optional).
- `CreatedAt` is optional.

Rule entry that parses in both SDKs, SqlFilter + SqlRuleAction:

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><id>https://localhost:5300/t/Subscriptions/s/Rules/sqlrule?api-version=2024-05</id><title type="text">sqlrule</title><published>2024-01-02T03:04:05.1234567Z</published><updated>2024-01-02T03:04:05.1234567Z</updated><author><name>ns</name></author><link rel="self" href="https://localhost:5300/t/Subscriptions/s/Rules/sqlrule?api-version=2024-05"/><content type="application/xml"><RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><Filter i:type="SqlFilter"><SqlExpression>color = @c</SqlExpression><CompatibilityLevel>20</CompatibilityLevel><Parameters><KeyValueOfstringanyType><Key>@c</Key><Value i:type="d6p1:string" xmlns:d6p1="http://www.w3.org/2001/XMLSchema">blue</Value></KeyValueOfstringanyType></Parameters></Filter><Action i:type="SqlRuleAction"><SqlExpression>SET x = 1</SqlExpression><CompatibilityLevel>20</CompatibilityLevel></Action><CreatedAt>2024-01-02T03:04:05.1234567Z</CreatedAt><Name>sqlrule</Name></RuleDescription></content></entry>
```

The CorrelationFilter (with properties) and TrueFilter variants have the same envelope, with
these `RuleDescription` children:

```xml
<Filter i:type="CorrelationFilter"><CorrelationId>cid</CorrelationId><Label>subj</Label><Properties><KeyValueOfstringanyType><Key>color</Key><Value i:type="d6p1:string" xmlns:d6p1="http://www.w3.org/2001/XMLSchema">red</Value></KeyValueOfstringanyType><KeyValueOfstringanyType><Key>n</Key><Value i:type="d6p1:int" xmlns:d6p1="http://www.w3.org/2001/XMLSchema">7</Value></KeyValueOfstringanyType></Properties></Filter><Action i:type="EmptyRuleAction"/><CreatedAt>…</CreatedAt><Name>corrrule</Name>

<Filter i:type="TrueFilter"><SqlExpression>1=1</SqlExpression><CompatibilityLevel>20</CompatibilityLevel></Filter><Action i:type="EmptyRuleAction"/><CreatedAt>…</CreatedAt><Name>$Default</Name>
```

**[captured]** How the SDKs read these back:

- JS → `{"name":"corrrule","filter":{"correlationId":"cid","subject":"subj","applicationProperties":{"color":"red","n":7}},"action":{}}`.
- Go → `CorrelationFilter{CorrelationID:"cid", Subject:"subj", ApplicationProperties:{"color":"red","n":int64(7)}}` with Action nil.
- `EmptyRuleAction` maps to a nil Action in Go and `{}` in JS.

### 3.6 Feeds

List feed with self + next links. This one is page 1 of `$top=2` over 3 queues:

```xml
<feed xmlns="http://www.w3.org/2005/Atom"><title type="text">Queues</title><id>https://localhost:5300/$Resources/Queues?api-version=2024-05</id><updated>2024-01-02T03:04:05.1234567Z</updated><link rel="self" href="https://localhost:5300/$Resources/Queues?api-version=2024-05&amp;$skip=0&amp;$top=2"/><link rel="next" href="https://localhost:5300/$Resources/Queues?api-version=2024-05&amp;$skip=2&amp;$top=2"/><entry xmlns="http://www.w3.org/2005/Atom"><id>https://localhost:5300/q0?api-version=2024-05</id><title type="text">q0</title>…<content type="application/xml"><QueueDescription …>…</QueueDescription></content></entry><entry>…q1…</entry></feed>
```

- Feed titles: `Queues`, `Topics`, `Subscriptions`, `Rules`. Neither SDK reads them, except for
  Go's not-found check below.
- Each entry is identical to the single-entity entry: absolute `<id>`, `<title>` = name, and full
  content.
- For **list-runtime-properties** the same feed serves both list and runtime calls, so every entry
  needs **both** the static required fields and CountDetails/MessageCount/CreatedAt/UpdatedAt/AccessedAt.
- In fact every GET must always return the union, because `getQueue` and
  `getQueueRuntimeProperties` hit the same URL.
- Last page: omit `rel="next"`; keep `rel="self"`.
- **Empty list**: a feed with no `<entry>`, e.g.
  `<feed xmlns="http://www.w3.org/2005/Atom"><title type="text">Queues</title><id>…</id><updated>…</updated><link rel="self" href="…"/></feed>`.

**Not-found "empty feed"** (alternative to a 404 for single-entity GETs):

```xml
<feed xmlns="http://www.w3.org/2005/Atom"><title type="text">Publicly Listed Services</title><subtitle type="text">This is the list of publicly-listed services currently available.</subtitle><id>uuid:0b8c2b4e-0000-0000-0000-000000000000;id=1</id><updated>2024-01-02T03:04:05.1234567Z</updated><generator>Microsoft® Service Bus</generator></feed>
```

- Go recognises it only after failing to unmarshal an `<entry>`, and then only if the `<title>`
  text is **exactly** `Publicly Listed Services` (`entity_manager.go:228-252`). Any other feed
  title gives `expected element type <entry> but have <feed>`.
- JS treats any feed with no entries on a single-entity GET as not-found
  (`serviceBusAtomManagementClient.js:1200-1206`).
- **Recommendation:** use **404 + `<Error>`** for single-entity GETs. It is simpler and gives the
  same result in both SDKs: Go returns `(nil, nil)` and JS throws `MessageEntityNotFoundError`.

### 3.7 Captured queue GET response (works in both SDKs)

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><id>https://localhost:5300/myqueue?api-version=2024-05</id><title type="text">myqueue</title><published>2024-01-02T03:04:05.1234567Z</published><updated>2024-01-02T03:04:05.1234567Z</updated><author><name>ns</name></author><link rel="self" href="https://localhost:5300/myqueue?api-version=2024-05"/><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><LockDuration>PT1M</LockDuration><MaxSizeInMegabytes>1024</MaxSizeInMegabytes><RequiresDuplicateDetection>false</RequiresDuplicateDetection><RequiresSession>false</RequiresSession><DefaultMessageTimeToLive>P10675199DT2H48M5.4775807S</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>false</DeadLetteringOnMessageExpiration><DuplicateDetectionHistoryTimeWindow>PT10M</DuplicateDetectionHistoryTimeWindow><MaxDeliveryCount>10</MaxDeliveryCount><EnableBatchedOperations>true</EnableBatchedOperations><SizeInBytes>0</SizeInBytes><MessageCount>3</MessageCount><IsAnonymousAccessible>false</IsAnonymousAccessible><AuthorizationRules></AuthorizationRules><Status>Active</Status><ForwardTo>sb://localhost/other</ForwardTo><CreatedAt>2024-01-02T03:04:05.1234567Z</CreatedAt><UpdatedAt>2024-01-02T03:04:05.1234567Z</UpdatedAt><AccessedAt>0001-01-01T00:00:00Z</AccessedAt><SupportOrdering>true</SupportOrdering><CountDetails xmlns:d2p1="http://schemas.microsoft.com/netservices/2011/06/servicebus"><d2p1:ActiveMessageCount>3</d2p1:ActiveMessageCount><d2p1:DeadLetterMessageCount>1</d2p1:DeadLetterMessageCount><d2p1:ScheduledMessageCount>2</d2p1:ScheduledMessageCount><d2p1:TransferMessageCount>0</d2p1:TransferMessageCount><d2p1:TransferDeadLetterMessageCount>0</d2p1:TransferDeadLetterMessageCount></CountDetails><AutoDeleteOnIdle>P10675199DT2H48M5.4775807S</AutoDeleteOnIdle><EnablePartitioning>false</EnablePartitioning><EntityAvailabilityStatus>Available</EntityAvailabilityStatus><EnableExpress>false</EnableExpress><UserMetadata>123</UserMetadata><MaxMessageSizeInKilobytes>256</MaxMessageSizeInKilobytes></QueueDescription></content></entry>
```

The Azure-sourced Go fixture `internal/atom/testdata/queue.xml` has the same shape, with a
populated AuthorizationRules block using `i:type="SharedAccessAuthorizationRule"`.

How the SDKs read it **[captured]**:

- JS getQueue → `{"name":"myqueue","forwardTo":"sb://localhost/other","userMetadata":"123","lockDuration":"PT1M","maxSizeInMegabytes":1024,…,"status":"Active","enableExpress":false,"availabilityStatus":"Available","maxMessageSizeInKilobytes":256}`.
- JS getQueueRuntimeProperties → `{"sizeInBytes":0,"totalMessageCount":3,"activeMessageCount":3,"deadLetterMessageCount":1,"scheduledMessageCount":2,…,"accessedAt":"0001-01-01T00:00:00.000Z"}`.
- Go runtime → `TotalMessageCount:3 ActiveMessageCount:3 DeadLetterMessageCount:1 ScheduledMessageCount:2 AccessedAt:0001-01-01 00:00:00 UTC`.

Empty `<AuthorizationRules></AuthorizationRules>` is fine in both: JS → undefined, Go → [].

### 3.8 Topic and subscription bodies (runtime-capable)

TopicDescription content that satisfies both SDKs for get, runtime and list:

```xml
<TopicDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><DefaultMessageTimeToLive>P10675199DT2H48M5.4775807S</DefaultMessageTimeToLive><MaxSizeInMegabytes>1024</MaxSizeInMegabytes><RequiresDuplicateDetection>false</RequiresDuplicateDetection><DuplicateDetectionHistoryTimeWindow>PT10M</DuplicateDetectionHistoryTimeWindow><EnableBatchedOperations>true</EnableBatchedOperations><SizeInBytes>0</SizeInBytes><FilteringMessagesBeforePublishing>false</FilteringMessagesBeforePublishing><IsAnonymousAccessible>false</IsAnonymousAccessible><AuthorizationRules></AuthorizationRules><Status>Active</Status><CreatedAt>…Z</CreatedAt><UpdatedAt>…Z</UpdatedAt><AccessedAt>0001-01-01T00:00:00Z</AccessedAt><SupportOrdering>true</SupportOrdering><CountDetails xmlns:d2p1="http://schemas.microsoft.com/netservices/2011/06/servicebus">…5 children…</CountDetails><SubscriptionCount>2</SubscriptionCount><AutoDeleteOnIdle>P10675199DT2H48M5.4775807S</AutoDeleteOnIdle><EnablePartitioning>false</EnablePartitioning><IsExpress>false</IsExpress><EntityAvailabilityStatus>Available</EntityAvailabilityStatus><EnableSubscriptionPartitioning>false</EnableSubscriptionPartitioning><EnableExpress>false</EnableExpress><SqlFilterCount>1</SqlFilterCount><CorrelationFilterCount>1</CorrelationFilterCount></TopicDescription>
```

SqlFilterCount and CorrelationFilterCount are new in api 2024-05:

- Go reads them (`admin_client_topic.go:433-434`); missing means 0.
- JS reads `SqlFilterCount` / `CorrelationFilterCount`; missing means undefined.
- Per Go docs, SqlFilterCount counts the default TrueFilter rules too (`admin_client_topic.go:80-84`).
- **[code]** Go maps Filter type `TrueFilter` to `admin.TrueFilter`.

SubscriptionDescription content for get, runtime and list:

```xml
<SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><LockDuration>PT1M</LockDuration><RequiresSession>false</RequiresSession><DefaultMessageTimeToLive>P10675199DT2H48M5.4775807S</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>false</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>true</DeadLetteringOnFilterEvaluationExceptions><MessageCount>5</MessageCount><MaxDeliveryCount>10</MaxDeliveryCount><EnableBatchedOperations>true</EnableBatchedOperations><Status>Active</Status><CreatedAt>…Z</CreatedAt><UpdatedAt>…Z</UpdatedAt><AccessedAt>…Z</AccessedAt><CountDetails xmlns:d2p1="http://schemas.microsoft.com/netservices/2011/06/servicebus"><d2p1:ActiveMessageCount>5</d2p1:ActiveMessageCount><d2p1:DeadLetterMessageCount>1</d2p1:DeadLetterMessageCount><d2p1:ScheduledMessageCount>2</d2p1:ScheduledMessageCount><d2p1:TransferMessageCount>0</d2p1:TransferMessageCount><d2p1:TransferDeadLetterMessageCount>0</d2p1:TransferDeadLetterMessageCount></CountDetails><AutoDeleteOnIdle>P10675199DT2H48M5.4775807S</AutoDeleteOnIdle><EntityAvailabilityStatus>Available</EntityAvailabilityStatus></SubscriptionDescription>
```

- Add `<ForwardTo>`, `<ForwardDeadLetteredMessagesTo>` and `<UserMetadata>` when set.
- Subscription entries need `<id>https://{host}/{topic}/Subscriptions/{sub}?api-version=2024-05</id>`;
  JS gets topicName and subscriptionName from it.

### 3.9 Namespace info response

```xml
<entry xmlns="http://www.w3.org/2005/Atom"><id>https://localhost:5300/$namespaceinfo?api-version=2024-05</id><title type="text">ns</title><updated>…</updated><author><name>ns</name></author><link rel="self" href="https://localhost:5300/$namespaceinfo?api-version=2024-05"/><content type="application/xml"><NamespaceInfo xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance"><CreatedTime>2024-01-02T03:04:05.1234567Z</CreatedTime><MessagingSKU>Standard</MessagingSKU><ModifiedTime>2024-01-02T03:04:05.1234567Z</ModifiedTime><Name>ns</Name><NamespaceType>Messaging</NamespaceType></NamespaceInfo></content></entry>
```

If you report `Premium`, you must add `<MessagingUnits>1</MessagingUnits>`.

### 3.10 Formats

- **Timestamps** (`CreatedAt`, `UpdatedAt`, `AccessedAt`, `CreatedTime`, `ModifiedTime`, `published`, `updated`):
  RFC 3339 UTC with `Z`, any fraction length (`2024-01-02T03:04:05.1234567Z` parses in both SDKs).
  - Never empty: Go fails on `""`.
  - "Never accessed": use `0001-01-01T00:00:00Z`. It parses in Go (zero time) and in JS (year 1).
    Go also special-cases `0001-01-01T00:00:00` without the Z.
- **Durations**: as in §2.1. The infinite value is `P10675199DT2H48M5.4775807S`.
  - Default values for unspecified fields, Azure-like:
    - LockDuration `PT1M`;
    - DefaultMessageTimeToLive and AutoDeleteOnIdle infinite;
    - DuplicateDetectionHistoryTimeWindow `PT10M`;
    - MaxDeliveryCount `10`;
    - MaxSizeInMegabytes `1024`;
    - EnableBatchedOperations `true`;
    - Status `Active`;
    - EntityAvailabilityStatus `Available`;
    - EnableExpress `false`.
  - These are not SDK requirements, except that JS requires the elements to be **present**.
- **Booleans**: lowercase `true`/`false`.
- **UserMetadata**: plain text. JS returns the raw string (`parseTagValue: false`), so `123` stays
  `"123"`. Emit an empty or absent element when unset.

---

## 4. Per-SDK operation catalogue (for test coverage)

### Go `admin.Client`

| Method | Returns |
|---|---|
| `GetNamespaceProperties` | |
| `CreateQueue` | |
| `UpdateQueue` | |
| `GetQueue` | nil for 404 / empty feed |
| `GetQueueRuntimeProperties` | nil for 404 / empty feed |
| `DeleteQueue` | **error** on 404 |
| `NewListQueuesPager` | |
| `NewListQueuesRuntimePropertiesPager` | |
| `CreateTopic`, `UpdateTopic`, `GetTopic`, `GetTopicRuntimeProperties`, `DeleteTopic`, `NewListTopicsPager`, `NewListTopicsRuntimePropertiesPager` | |
| `CreateSubscription`, `UpdateSubscription`, `GetSubscription`, `GetSubscriptionRuntimeProperties`, `DeleteSubscription`, `NewListSubscriptionsPager`, `NewListSubscriptionsRuntimePropertiesPager` | |
| `CreateRule` | **panics if options is nil**: dereferences `options.Filter` (`admin_client_rules.go:178-182`) |
| `GetRule`, `UpdateRule`, `DeleteRule`, `NewListRulesPager` | |

Go has **no** `*Exists` helpers.

### JS `ServiceBusAdministrationClient`

| Methods | Notes |
|---|---|
| `getNamespaceProperties` | |
| `createQueue`, `getQueue`, `getQueueRuntimeProperties`, `listQueues`, `listQueuesRuntimeProperties`, `updateQueue`, `deleteQueue`, `queueExists` | `update*` throws client-side if `name` is missing; it never reaches the server |
| the same set for topics | |
| `createSubscription`, `getSubscription`, `getSubscriptionRuntimeProperties`, `listSubscriptions`, `listSubscriptionsRuntimeProperties`, `updateSubscription`, `deleteSubscription`, `subscriptionExists` | |
| `createRule(topic, sub, name, filter, [action])`, `getRule`, `listRules`, `updateRule`, `deleteRule`, `ruleExists` | |
| `*Exists` | returns `false` on **any** error, including PARSE_ERROR from a malformed response, so a broken response looks like "not found" |
| `get*s` | deprecated internal page functions behind `list*` |

### Wrong-type lookups

If the server returns a topic entry for a GET on `/{name}`, e.g. `getQueue("sometopic")`:

- Go returns a queue response with all-nil fields;
- JS throws PARSE_ERROR (missing LockDuration), so `queueExists` returns false.

For the emulator, returning **404 when the entity at that path is not of the requested kind is
impossible**, because the URL carries no type. Return the actual entity, as Azure does.

---

## 5. Open items not settled by the SDK code

- **Update semantics, merge vs replace** (§2.2).
- **Go no-name `CreateRule` PUT to `…/Rules/`** (§1.2).
- **Page-size cap when `$top` is absent.** Returning everything is safest.
- **Listing subscriptions or rules of a non-existent topic or subscription.** Both SDKs surface a
  404 as an error and an empty feed as an empty list. Azure's exact behaviour is not visible from
  the SDKs.
- **JS `UseDevelopmentEmulator=false` still disabling TLS** is [code] only.
