// package botbot is an API binding for botbot.me.
//
// the botbot base URL is https://botbot.me/.
//
// the URL pattern for accessing plain-text logs is
// https://botbot.me/<network>/<channel>/<year>-<month>-<date>.log?page=<n>, where page starts at 1, and ends when the client receives a 404.
package botbot

import (
	"bufio"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Message struct {
	Timestamp time.Time
	User      string
	Text      string
}

func getpage(ctx context.Context, network, channel string, date time.Time, page int) ([]*Message, bool, error) {
	url := fmt.Sprintf("https://botbot.me/%s/%s/%s.log?page=%d", network, channel, date.Format("2006-01-02"), page)

	tr := &http.Transport{}
	cl := &http.Client{Transport: tr}

	resp, err := ctxhttp.Get(ctx, cl, url)
	if err != nil {
		return nil, false, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("unhandled response status %q", resp.Status)
	}

	year, month, day := date.Date()

	sc := bufio.NewScanner(resp.Body)

	msgs := []*Message{}

	txt := false

	for sc.Scan() {
		txt = true
		l := sc.Text()
		spl := strings.SplitN(l, " ", 3)

		// skip join/part/nick/action
		if spl[1] == "*" {
			continue
		}

		t, err := time.Parse("15:04:05", strings.Trim(spl[0], "[]"))
		if err != nil {
			return nil, false, fmt.Errorf("failed to parse message %q: %v", l, err)
		}

		user := strings.Trim(spl[1], "<>")
		txt := spl[2]

		m := &Message{
			Timestamp: time.Date(year, month, day, t.Hour(), t.Minute(), t.Second(), 0, time.UTC),
			User:      user,
			Text:      txt,
		}

		msgs = append(msgs, m)
	}

	return msgs, txt, nil
}

func GetDate(ctx context.Context, network, channel string, day time.Time) ([]*Message, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var msgs []*Message

	mc, ec := GetMessageStreamDate(ctx, network, channel, day)

	for m := range mc {
		msgs = append(msgs, m)
	}

	if err := <-ec; err != nil {
		return nil, err
	}

	return msgs, nil
}

// GetMessages gets messsages from a given botbot.me network, channel and date.
func GetMessageStreamDate(ctx context.Context, network, channel string, day time.Time) (<-chan *Message, <-chan error) {
	c := make(chan *Message, 20)
	errc := make(chan error, 1)

	go func() {
		page := 1
		defer close(c)
		defer close(errc)
		for {
			msgs, ok, err := getpage(ctx, network, channel, day, page)
			if err != nil {
				errc <- err
				return
			}

			if !ok {
				break
			}

			for _, m := range msgs {
				select {
				case <-ctx.Done():
					errc <- ctx.Err()
					return
				case c <- m:
				}
			}

			page++
		}
	}()

	return c, errc
}

func GetMessageStreamMonth(ctx context.Context, network, channel string, year int, month time.Month) (<-chan *Message, <-chan error) {
	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup

	mch := make(chan *Message, 20)
	oerrc := make(chan error, 1)

	div := 8
	chlen := (31 / div) + 1

	ch := make(chan (<-chan *Message), chlen)
	derrc := make(chan error, chlen)

	t := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	st := t
	for st.Month() == month {
		wg.Add(1)
		en := st.AddDate(0, 0, div-1)
		if en.Month() != month {
			en = en.AddDate(0, 0, -en.Day())
		}

		och := make(chan *Message, 20)
		ch <- och

		go func(start, end time.Time, o chan *Message) {
			defer wg.Done()
			defer close(o)

			for start.Month() == month && start.Before(end) {
				mc, ec := GetMessageStreamDate(ctx, network, channel, start)

				for m := range mc {
					select {
					case <-ctx.Done():
						return
					case o <- m:
					}
				}

				if err := <-ec; err != nil {
					cancel()
					derrc <- err
					return
				}
				start = start.AddDate(0, 0, 1)
			}
		}(st, en, och)

		if en.AddDate(0, 0, 1).Month() != month {
			break
		}

		st = st.AddDate(0, 0, div)
	}

	go func() {
		wg.Wait()
		close(ch)
		close(derrc)
	}()

	go func() {
		defer close(mch)
		defer close(oerrc)
		for mc := range ch {
			for m := range mc {
				select {
				case mch <- m:
				case <-ctx.Done():
					oerrc <- ctx.Err()
					cancel()
					return
				case err := <-derrc:
					oerrc <- err
					cancel()
					return
				}
			}
		}

		if err := <-derrc; err != nil {
			oerrc <- err
		}
	}()

	return mch, oerrc
}
