(ns nucalc.util
  (:require [clojure.core.async :as async]))

(defn debounce [in ms]
  (let [out (async/chan)]
    (async/go-loop [last-val nil]
      (let [val (if (nil? last-val) (async/<! in) last-val)
            timer (async/timeout ms)
            [new-val ch] (async/alts! [in timer])]
        (condp = ch
          timer (do (async/>! out val) (recur nil))
          in (recur new-val))))
    out))


(defn debounced-function [minimum-pause-in-milliseconds function]
  (let [input-channel (async/chan)
        debounced-input-channel (debounce input-channel
                                          minimum-pause-in-milliseconds)]
    (async/go-loop []
      (let [input (async/<! debounced-input-channel)]
        (apply function input)
        (recur)))

    (fn [& input]
      (async/>!! input-channel input))))

(comment

  (def a-function (debounced-function 1000
                                      (fn [x] (prn 'x-is x))))

  (a-function 1)

  ) ;; TODO: remove-me
