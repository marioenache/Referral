package Me.Teenaapje.Referral.Utils;

public class TopPlayer {
	public String playerUUID;
	public String playerName;
	public int totalRefers;
	public int playerPos;

	public TopPlayer(String playerUUID, String playerName, int totalRefers, int playerPos) {
		this.playerUUID = playerUUID;
		this.playerName = playerName;
		this.totalRefers = totalRefers;
		this.playerPos = playerPos;
	}
}