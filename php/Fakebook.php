<?php
require_once 'php/Predis.php';

class Fakebook {

	private $r;

	function __construct() {
		$this->r = new Predis\Client();
	}

	/**
	 * Generate a new random string to use as autentication token
	 * @return string Random token of 32 alphanumeric characters
	 */
	static function generateToken() {
		$length = 32;
		$characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
		$token = '';
		for ($i = 0; $i < $length; $i++) {
			$token .= $characters[rand(0, strlen($characters) - 1)];
		}
		return $token;
	}

	function error($code = 7) {
		header("location: error.php?code=$code");;
	}

	function ok($code) {
		header("location: ok.php?code=$code");;
	}

	/**
	 * Signup a new user
	 * TODO what if the user already exists?
	 * @param  array $userData The new user data
	 */
	function signup($userData) {
		$id = $this->r->incr('global:nextUserId');
		$token = Fakebook::generateToken();
		$email = $userData['email'];

		$this->r->set("uid:$id:email", $userData['email']);
		$this->r->set("uid:$id:password", $userData['password']);
		$this->r->set("uid:$id:name", $userData['name']);
		$this->r->set("uid:$id:surname", $userData['surname']);
		$this->r->set("uid:$id:token", $token);
		$this->r->set("token:$token", $id);
		$this->r->set("email:$email:uid", $id);
	}

	/**
	 * Login a user, it uses cookies so it will not work if the header is already sent
	 * @param  string $email    E-mail of the user trying to login
	 * @param  string $password Password of the user trying to login
	 * @return boolean           true if login is succesful, false otherwise
	 */
	function login($email, $password) {
		$userId = $this->r->get("email:$email:uid");
		if (!$userId) {
			return false;
		}
		$storedPassword = $this->r->get("uid:$userId:password");
		if ($password != $storedPassword) {
			return false;
		}
		// email and password are ok, generate the token cookie
		$token = $this->r->get("uid:$userId:token");
		setcookie('token', $token, time()+3600*24*365);
		return true;
	}

	/**
	 * Check if the user is logged, it looks for the token string in the cookie
	 * @return int  	user id, 0 if is not logged
	 */
	function getLoggedUserId() {
		if (isset($_COOKIE['token'])) {
			$clientToken = $_COOKIE['token'];
			if ($userId = $this->r->get("token:$clientToken")) {
				if ($clientToken == $this->r->get("uid:$userId:token")) {
					return $userId;
				}
			}
		}
		return 0;
	}

	/**
	 * Logout the user and set a new token string
	 */
	function logout() {
		if ($userId = $this->getLoggedUserId()) {
			$newToken = Fakebook::generateToken();
			$oldToken = $this->r->get("uid:$userId:token");

			$this->r->set("uid:$userId:token", $newToken);
			$this->r->set("token:$newToken", $userId);
			$this->r->del("token:$oldToken");
		}
	}

	/**
	 * Get the full name of the user logged in
	 * @return string full name
	 */
	function getFullName($uid = 0) {
		if ($uid == 0) {
			$uid = $this->getLoggedUserId();
		}
		if ($uid == 0) {
			return;
		}
		return $this->r->get("uid:$uid:name") . " " . $this->r->get("uid:$uid:surname");
	}

	/**
	 * Request friendship
	 * @param  int 		$uid   	id of the username to request the friendship to
	 * @return  boolean	   		true if the requested is sent, false otherwise
	 */
	function addFriendRequest($uid) {
		$myid = $this->getLoggedUserId();
		// check if they are already friends
		if ($this->r->sismember("uid:$myid:friends", $uid)) {
			return false;
		}
		// add the request
		$this->r->sadd("uid:$uid:friendrequests", $myid);
		return true;
	}

	/**
	 * Remove friendship request
	 * @param  int 		$uid   	id of the username to refuse
	 */
	function removeFriendRequest($uid) {
		$myid = $this->getLoggedUserId();
		$this->r->srem("uid:$myid:friendrequests", $uid);
	}

	/**
	 * Accept a friend request
	 * @param  int 		$uid   	id of the username to accept
	 * @return boolean 		    true if the friendship is setted, false otherwise
	 */
	function acceptFriendRequest($uid) {
		$myid = $this->getLoggedUserId();
		// check if there is a request
		if (!$this->r->sismember("uid:$myid:friendrequests", $uid)) {
			return false;
		}
		// add to friends (both sides)
		$this->r->sadd("uid:$myid:friends", $uid);
		$this->r->sadd("uid:$uid:friends", $myid);
		// remove the request (both sides)
		$this->r->srem("uid:$uid:friendrequests", $myid);
		$this->r->srem("uid:$myid:friendrequests", $uid);

		return true;
	}

	function getFriendRequests() {
		$uid = $this->getLoggedUserId();
		return $this->r->smembers("uid:$uid:friendrequests");
	}

	function getUpdates() {
		$uid = $this->getLoggedUserId();
		return $this->r->lrange("uid:$uid:updates", 0, 100);
	}

	function getStatuses($uid) {
		return $this->r->lrange("uid:$uid:statuses", 0, 100);
	}

	/**
	 * Remove friendship
	 * @param  int    $user1 	user id
	 * @param  int    $user2 	user id
	 */
	function removeFriendship($user1, $user2) {
		$this->r->srem("uid:$user1:friends", $user2);
		$this->r->srem("uid:$user2:friends", $user1);
	}

	function userExists($uid) {
		return $this->r->exists("uid:$uid:name");
	}

	function isFriend($userId) {
		if ($myUserId = $this->getLoggedUserId()) {
			return $this->r->sismember("uid:$myUserId:friends", $userId);
		}
		return false;
	}

	/**
	 * Publish a new user status
	 * @param  string 	$message 	status message
	 */
	function pushStatus($message) {
		// get the logged user id
		$uid = $this->getLoggedUserId();
		// get the post id
		$pid = $this->r->incr('global:nextPostId');
		// build the status string
		$status = $pid . '|' . $uid . '|' . $message . '|' . time();
		// push the status in the user statuses list
		$this->r->rpush("uid:$uid:statuses", $status);
		// push the status in every friend's updates list
		$friends = $this->r->smembers("uid:$uid:friends");
		foreach ($friends as $fid) {
			$this->r->rpush("uid:$fid:updates", $status);
		}
	}

	function timeAgo($ptime) {
		$etime = time() - $ptime;

		if ($etime < 1)
		{
			return '0 seconds';
		}

		$a = array( 12 * 30 * 24 * 60 * 60  =>  'year',
			30 * 24 * 60 * 60       =>  'month',
			24 * 60 * 60            =>  'day',
			60 * 60                 =>  'hour',
			60                      =>  'minute',
			1                       =>  'second'
			);

		foreach ($a as $secs => $str)
		{
			$d = $etime / $secs;
			if ($d >= 1)
			{
				$r = round($d);
				return $r . ' ' . $str . ($r > 1 ? 's' : '') . ' ago';
			}
		}
	}
}