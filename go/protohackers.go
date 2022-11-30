package main

import (
	"net"
	"protohackers/line_reversal"
	"strings"

	"github.com/caarlos0/log"
)

func lstn(connection *net.UDPConn, alarm chan string, lrs *line_reversal.LRServer) {
	buffer := make([]byte, 1024)
	n, remoteAddr, err := 0, new(net.UDPAddr), error(nil)
	for err == nil {
		n, remoteAddr, err = connection.ReadFromUDP(buffer)
		if err != nil {
			log.WithField("error", err).Error("Error reading from UDP")
		} else {
			log.WithFields(log.Fields{"remote": remoteAddr, "data": strings.ReplaceAll(string(buffer[:n]), "\n", "\\n")}).Info("Received message")
		}

		messages := lrs.HandleRequest(connection, buffer[:n], remoteAddr)

		for _, msg := range messages {
			_, err = connection.WriteToUDP([]byte(msg), remoteAddr)
			if err != nil {
				log.WithField("error", err).Error("Error writing to UDP")
			} else {
				log.WithFields(log.Fields{"remote": remoteAddr, "data": "OK"}).Info("Sent message")
			}
		}
	}

	log.WithFields(log.Fields{"n": n, "remoteAddr": remoteAddr, "err": err}).Error("UDP Server Error")
	alarm <- "Listener failed! ( " + err.Error() + " )"
}

func send(connection *net.UDPConn, alarm chan string) {

}

func main() {
	addr := net.UDPAddr{
		Port: 10008,
		IP:   net.ParseIP("0.0.0.0"),
	}
	connection, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.WithField("error", err).Fatal("Error listening on UDP")
	}
	log.WithField("addr", addr).Info("Listening on UDP")
	alarm := make(chan string)
	lrs := line_reversal.New(connection)
	go lstn(connection, alarm, lrs)

	msg := <-alarm
	log.Error(msg)
}
