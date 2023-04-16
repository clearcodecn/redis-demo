package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func main() {
	setDemo()
}

// zset:  [v1, v2, v3, v4]
func setDemo() {
	cli, _ := getConnection()
	//zz, err := cli.ZRangeWithScores(context.Background(), "zset.mykey", 0, 3).Result()
	//if err != nil {
	//	return
	//}
	//for _, z := range zz {
	//	fmt.Println(z.Member)
	//}

	go func() {
		tk := time.NewTicker(1 * time.Second)
		for range tk.C {
			ss, err := cli.ZRangeByScore(context.Background(), "zset.order3", &redis.ZRangeBy{
				Min:    "-inf",
				Max:    strconv.Itoa(int(time.Now().Unix())),
				Offset: 0,
				Count:  1,
			}).Result()
			_ = err

			if len(ss) == 0 {
				continue
			}

			fmt.Println(string(ss[0]))
			cli.ZRem(context.Background(), "zset.order3", ss[0])
		}
	}()

	for i := 0; i < 10; i++ {
		// 1. 用户下单.
		order := &Order{
			ExpireTime: time.Now().Add(10 * time.Second).Unix(),
			OrderNo:    fmt.Sprintf("order%d", i),
		}
		bs, _ := json.Marshal(order)
		cli.ZAdd(context.Background(), "zset.order3", redis.Z{
			Score:  float64(order.ExpireTime),
			Member: string(bs),
		})
		fmt.Println(i, order.ExpireTime)
		time.Sleep(1 * time.Second)
	}

	select {}
}

type Order struct {
	ExpireTime int64  `json:"expire_time"`
	OrderNo    string `json:"order_no"`
}

// key:  {key1:valu1, key2: valu2}
func hashMapDemo() {
	cli, _ := getConnection()
	cli.HSet(context.Background(), "hmap", "key1", "val1", "key2", "val2")
	cli.HDel(context.Background(), "hmap", "key1")
}

func pubsub() {
	cli, _ := getConnection()

	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			cli.Publish(context.Background(), "mq.pub", &User{
				Username: fmt.Sprintf("%d", time.Now().UnixNano()),
			})
		}
	}()

	for i := 0; i < 10; i++ {
		go func() {
			pubsub := cli.Subscribe(context.Background(), "mq.pub")
			for {
				msg, err := pubsub.ReceiveMessage(context.Background())
				if err != nil {
					panic(err)
				}
				fmt.Println(msg.Payload)
			}
		}()
	}
	select {}
}

// key -> [ 1, 2 ,3 ,4, 1,2,3,4]
func listFunc() {
	cli, _ := getConnection()

	// mq
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			cli.LPush(context.Background(), "mq", &User{
				Username: fmt.Sprintf("%s", time.Now().String()),
			})
		}
	}()

	for i := 0; i < 10; i++ {
		go func() {
			for {
				time.Sleep(250 * time.Millisecond)
				data, err := cli.RPop(context.Background(), "mq").Result()
				if err == redis.Nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				fmt.Println(data, err)
			}
		}()
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			res, err := cli.LLen(context.Background(), "mq").Result()
			_ = err
			fmt.Println("当前队列的数据还剩余: ", res)
		}
	}()

	select {}
}

func nxFuncs() {
	var wg sync.WaitGroup
	stoke := 4
	for i := 0; i < 10; i++ {
		wg.Add(1)
		user := i
		go func() {
			defer wg.Done()

			if err := nxLock(5*time.Second, func() {
				// get lock success.
				fmt.Printf("第 %d 位 客户 get lock success\n", user)
				if stoke > 0 {
					stoke--
				} else {
					fmt.Printf("user 购买失败: %d\n", user)
				}
				dur := time.Duration(rand.Intn(2))
				time.Sleep(dur * time.Second)
			}); err != nil {
				log.Printf("%d get lock failed %v\n", user, err)
			}
		}()
	}

	wg.Wait()
	fmt.Println(stoke)
}

func nxLock(t time.Duration, callback func()) error {
	cli, err := getConnection()
	if err != nil {
		return err
	}
	dur := 30 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()

	for {
		set, err := cli.SetNX(ctx, "key", "1", t).Result()
		if err != nil {
			return err
		}
		if set {
			callback()
			// 释放锁.
			_, err := cli.Del(context.Background(), "key").Result()
			if err != nil {
				return err
			}
			break
		} else {
			time.Sleep(dur)
			// wait.
			// 第一次等 30ms
			// 第二次等 60ms . .... 120 .
			// 等到1s, 每次都只等1s钟.
			dur *= 2
			if dur >= 1*time.Second {
				dur = 1 * time.Second
			}
		}
	}
	return nil
}

func stringFuncs() {
	cli, err := getConnection()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	{
		res := cli.Set(ctx, "demo", "demo", 0)
		if res.Err() != nil {
			log.Fatal(res.Err())
		}

		res = cli.Set(ctx, "user", &User{Username: "user1"}, 0)
		if res.Err() != nil {
			log.Fatal(res.Err())
		}
	}

	{
		res, err := cli.Get(ctx, "demo").Result()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(res)
	}

	{
		cli.Set(ctx, "exp", "exp after 3 seconds", 3*time.Second)
		tk := time.NewTicker(1 * time.Second)
		for range tk.C {
			expres, err := cli.Get(ctx, "exp").Result()
			if err != nil {
				break
			}
			fmt.Println(expres)
		}
	}
}

type User struct {
	Username string `json:"username"`
}

func (u *User) MarshalBinary() (data []byte, err error) {
	return json.Marshal(u)
}

var (
	globalConn *redis.Client
	o          sync.Once
)

func getConnection() (*redis.Client, error) {
	o.Do(func() {
		rdb := redis.NewClient(&redis.Options{
			Addr:     "192.168.1.101:16379",
			Password: "redis", // no password set
			DB:       0,       // use default DB
		})

		if err := rdb.Ping(context.Background()).Err(); err != nil {
			panic(err)
		}

		globalConn = rdb
	})
	return globalConn, nil
}
