<div dir="rtl" style="text-align: right;">

# مستند طراحی سیستم توزیع شده با etcd

## مقدمه

این پروژه یک پایگاه داده توزیع شده در حافظه (Distributed In-Memory Database) است که از etcd برای مدیریت متادیتا و هماهنگی بین کامپوننت‌ها استفاده می‌کند. سیستم از معماری Single-Leader با تکرار (Replication) استفاده کرده و قابلیت تحمل خطا (Fault Tolerance) بالایی دارد.

## معماری کلی سیستم

</div>

<div dir="rlt" style="text-align: left;">

```
┌─────────────────────────────────────────────────────────────┐
│                    Controller Cluster                       │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │Controller-1 │    │Controller-2 │    │Controller-3 │      │
│  │  (Leader)   │    │(Follower)   │    │(Follower)   │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
└─────────────────────────────────────────────────────────────┘
                                        ▲
                                        │
                                        ▼
┌─────────────────┐                ┌─────────────────┐
│   Nodes Cluster │                │   etcd Cluster  │
│                 │                │                 │
│  ┌─────────────┐│                │┌─────────────┐  │
│  │   Node-1    ││                ││   etcd-1    │  │
│  │(Partition   ││                ││             │  │
│  │ 0,1)        ││                ││             │  │
│  └─────────────┘│                │└─────────────┘  │
│                 │ ◀─────────────▶│                 │
│  ┌─────────────┐│                │┌─────────────┐  │
│  │   Node-2    ││                ││   etcd-2    │  │
│  │(Partition   ││                ││             │  │
│  │ 1,2)        ││                ││             │  │
│  └─────────────┘│                │└─────────────┘  │
│                 │                │                 │
│  ┌─────────────┐│                │┌─────────────┐  │
│  │   Node-3    ││                ││   etcd-3    │  │
│  │(Partition   ││                ││             │  │
│  │ 0,2)        ││                ││             │  │
│  └─────────────┘│                │└─────────────┘  │
└─────────────────┘                └─────────────────┘
            ▲                         ▲
            │                         │
            │                         ▼
            │       ┌───────────────────┐
            └──────▶│   Load Balancers  │
                    │                   │
                    │  ┌─────────────┐  │
                    │  │Load Balancer│  │
                    │  │     1       │  │
                    │  └─────────────┘  │
                    │                   │
                    │  ┌─────────────┐  │
                    │  │Load Balancer│  │
                    │  │     2       │  │
                    │  └─────────────┘  │
                    └───────────────────┘
                                ▲
                                │
                                ▼
                    ┌───────────────────┐
                    │      Client       │
                    └───────────────────┘
```

</div>

<div dir="rtl" style="text-align: right;">

## چرا هیچ Single Point of Failure وجود ندارد؟

### ۱. خوشه etcd توزیع شده
- **۳ نود etcd**: سیستم از ۳ نود etcd تشکیل شده که با هم یک خوشه تشکیل می‌دهند
- **پروتکل Consensus**: etcd از Raft consensus algorithm استفاده می‌کند
- **تحمل خطا**: حتی اگر یکی از نودهای etcd از کار بیفتد، سیستم همچنان کار می‌کند
- **داده‌های تکرار شده**: تمام داده‌ها در تمام نودها تکرار می‌شوند

### ۲. Controller های چندگانه
- **انتخاب رهبر**: از etcd برای انتخاب رهبر استفاده می‌شود
- **Failover خودکار**: اگر Controller رهبر از کار بیفتد، یکی دیگر جایگزین می‌شود
- **هماهنگی**: تمام Controller ها از etcd برای هماهنگی استفاده می‌کنند

### ۳. Load Balancer های چندگانه
- **۲ Load Balancer**: سیستم از ۲ Load Balancer استفاده می‌کند
- **Failover**: اگر یکی از کار بیفتد، دیگری درخواست‌ها را مدیریت می‌کند
- **متادیتای مشترک**: هر دو از etcd برای دریافت متادیتای به‌روز استفاده می‌کنند

### ۴. تکرار داده‌ها در نودها
- **فاکتور تکرار**: هر پارتیشن در ۲ نود مختلف تکرار می‌شود
- **Leader-Follower**: هر پارتیشن یک Leader و یک Follower دارد
- **Failover خودکار**: اگر Leader از کار بیفتد، Follower جایگزین می‌شود

## استفاده از امکانات etcd

### ۱. انتخاب رهبر

<div dir="ltr" style="text-align: left;">

```go
session, err := concurrency.NewSession(cli, concurrency.WithTTL(5))
election := concurrency.NewElection(session, "controller/leader")
err := c.election.Campaign(context.Background(), fmt.Sprintf("controller-%d", c.id))
```

</div>

**مزایا:**
- فقط یک Controller در هر زمان رهبر است
- اگر رهبر از کار بیفتد، انتخابات جدید انجام می‌شود
- از race condition جلوگیری می‌کند

### ۲. قفل توزیع شده

<div dir="ltr" style="text-align: left;">

```go
txnResp, err := c.etcdClient.Txn(ctx).If(
    clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
).Then(
    clientv3.OpPut(key, string(nodeJSON)),
).Commit()
```

</div>

**مزایا:**
- عملیات اتومیک روی داده‌ها
- جلوگیری از تداخل بین Controller ها
- Consistency در داده‌ها

### ۳. ترنزکشن‌ها برای عملیات اتمیک

<div dir="ltr" style="text-align: left;">

```go
txnResp, err := c.etcdClient.Txn(ctx).If(
    clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
    clientv3.Compare(clientv3.Version(key), "=", 0),
).Then(
    clientv3.OpPut(key, string(nodeJSON)),
).Commit()

txnResp, err := c.etcdClient.Txn(ctx).If(
    clientv3.Compare(clientv3.CreateRevision(c.election.Key()), "=", c.election.Rev()),
    clientv3.Compare(
        clientv3.ModRevision(fmt.Sprintf("partitions/%d", partitionID)),
        "=",
        resp.Kvs[0].ModRevision,
    ),
).Then(
    clientv3.OpPut(
        fmt.Sprintf("partitions/%d", partitionID),
        c.updatePartitionData(partition, nodeID),
    ),
    clientv3.OpPut(
        fmt.Sprintf("nodes/%d/partitions", nodeID),
        c.updateNodePartitions(nodeID, partitionID),
    ),
).Commit()
```

</div>

**مزایای استفاده از ترنزکشن:**
- **عملیات اتمیک**: تمام عملیات در یک ترنزکشن یا موفق می‌شوند یا شکست می‌خورند
- **Consistency**: تضمین سازگاری داده‌ها حتی در صورت شکست
- **Isolation**: جلوگیری از تداخل بین عملیات همزمان
- **Durability**: تغییرات پس از commit در تمام نودها ذخیره می‌شوند

### ۴. ذخیره کلید-مقدار برای متادیتا

<div dir="ltr" style="text-align: left;">

```go
key := fmt.Sprintf("nodes/%d", nodeID)
_, err = c.etcdClient.Put(ctx, key, string(nodeJSON))

key := fmt.Sprintf("partitions/%d", partitionID)
_, err = c.etcdClient.Put(ctx, key, partitionJSON)
```

</div>

**ساختار کلیدها:**
- `nodes/{id}`: اطلاعات نودها
- `partitions/{id}`: اطلاعات پارتیشن‌ها
- `config/partition_count`: تعداد پارتیشن‌ها
- `config/replication_factor`: فاکتور تکرار

### ۵. نظارت بر تغییرات

<div dir="ltr" style="text-align: left;">

```go
ch := c.etcdClient.Watch(context.Background(), "nodes/active/", clientv3.WithPrefix())
for resp := range ch {
    for _, event := range resp.Events {
    }
}
```

</div>

**مزایا:**
- اطلاع فوری از تغییرات
- Failover خودکار
- هماهنگی real-time

### ۶. Session و Lease

<div dir="ltr" style="text-align: left;">

```go
session, err := concurrency.NewSession(n.etcdClient, concurrency.WithTTL(HEARTBEAT_TIMER))
n.etcdClient.Put(ctx, fmt.Sprintf("nodes/active/%d", n.Id), "alive", clientv3.WithLease(session.Lease()))
```

</div>

**مزایا:**
- تشخیص خودکار نودهای مرده
- Cleanup خودکار داده‌ها
- Heartbeat mechanism

## مکانیزم‌های تحمل خطا

### ۱. Controller Failover

<div dir="ltr" style="text-align: left;">

```go
if !txnResp.Succeeded {
    log.Printf("Not leader, cannot perform operation")
    return errors.New("not leader")
}
```

</div>

### ۲. Node Failover

<div dir="ltr" style="text-align: left;">

```go
if event.Type == clientv3.EventTypeDelete {
    go c.handleFailover(node.ID)
}
```

</div>

### ۳. تکرار پارتیشن

<div dir="ltr" style="text-align: left;">

```go
if len(partition.Replicas) < c.replicationFactor {
    partitionsToAssign = append(partitionsToAssign, partition.PartitionID)
}
```

</div>

### ۴. Load Balancer Failover
- اگر یکی از Load Balancer ها از کار بیفتد، دیگری درخواست‌ها را مدیریت می‌کند
- هر Load Balancer متادیتای خود را از etcd دریافت می‌کند

## مزایای استفاده از etcd

### ۱. سازگاری
- تمام کامپوننت‌ها از یک منبع حقیقت استفاده می‌کنند
- عملیات اتمیک و تراکنشی
- جلوگیری از race condition

### ۲. در دسترس بودن
- خوشه توزیع شده
- تحمل خطا در سطح بالا
- Failover خودکار

### ۳. مقیاس‌پذیری
- امکان اضافه کردن نودهای جدید
- توزیع بار بین نودها
- مدیریت پارتیشن‌ها

### ۴. قابلیت نظارت
- نظارت بر وضعیت نودها
- لاگ‌گیری از عملیات
- Debugging آسان

## نتیجه‌گیری

این معماری با استفاده از etcd، یک سیستم توزیع شده قوی و قابل اعتماد ایجاد کرده است که:

۱. **هیچ Single Point of Failure ندارد** - تمام کامپوننت‌ها تکرار شده‌اند
۲. **تحمل خطای بالا** - سیستم حتی با از کار افتادن چندین نود همچنان کار می‌کند
۳. **هماهنگی خودکار** - تمام کامپوننت‌ها از etcd برای هماهنگی استفاده می‌کنند
۴. **مقیاس‌پذیری** - امکان اضافه کردن نودهای جدید بدون توقف سیستم

استفاده از etcd در این پروژه نشان‌دهنده قدرت این تکنولوژی در ایجاد سیستم‌های توزیع شده قابل اعتماد است.

</div>