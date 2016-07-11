package soxrecorderv2.recorder;

import soxrecorderv2.util.PGConnectionManager;

/**
 * SOX Recorderシステムの中でサブコンポーネントとして動くもののインタフェース
 * @author tomotaka
 *
 */
public interface RecorderSubProcess {
	
	/**
	 * このRecorderSubProcessに終了処理を行わせる
	 * 実際は終了までブロックするわけではなく, 終了のフラグなどをたてたり
	 * 各RecorderSubProcessの内部スレッドを待ったりするための処理
	 */
	public void shutdownSubProcess();
	
	/**
	 * RecorderSubProcessがもっているPGConnectionManagerを取得する
	 * これは各RecorderSubProcessが処理を終了した上で安全にPostgreSQLとのコネクションを終了したいということから。
	 * @return
	 */
	public PGConnectionManager getConnManager();

}
