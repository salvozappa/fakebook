<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

$fakebook = new Fakebook();

if (isset($_POST['message'])) {
	$message = $_POST['message'];
	$to = $_POST['to'];
	$fakebook->sendMessage($to, $message);
	$fakebook->ok(5);
	return;
}

if (!isset($_GET['to'])) {
	$fakebook->error(4);
}

$uid = $_GET['to'];

?>
<!DOCTYPE html>
<html lang="en">
<?php include 'head.inc.php'; ?>
<body>

	<?php include 'topbar.inc.php'; ?>
	
	<div id="container">
		<?php include 'sidebar.inc.php' ?>
		<section id="content">
			<h1 style="margin-top: 0;">Send a message to <a href="user.php?id=<?= $uid ?>"><?= $fakebook->getFullName($uid) ?></a>:</h1>
			<form method="post" action="send-message.php">
				<input type="hidden" name="to" value="<?= $uid ?>">
				<textarea name="message" rows="4" cols="50"></textarea>
				<input type="submit">
			</form>
		</section>
	</div>
</body>
</html>