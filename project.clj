(defproject nucalc "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [camel-snake-kebab "0.4.1"]
                 [dali "0.1.0-SNAPSHOT"]
                 [org.clojure/data.csv "1.0.0"]
                 [medley "1.2.0"]
                 [me.raynes/fs "1.4.6"]
                 [com.taoensso/nippy "2.13.0"]]
  :main nucalc.core
;;  :aot [nucalc.core]
  )
