def query(particle: str):
    return f"""
    WITH filtered_particles AS (
	SELECT 
		multiIf (
			particleFrom != '{particle}', particleFrom,
			particleTo != '{particle}', particleTo,
			''
		) AS particle FROM spacebox.cyberlinks
	WHERE particleFrom = '{particle}' OR particleTo = '{particle}'
), linked_cyberlinks AS (
	SELECT 
		particle,
		particleFrom,
		particleTo,
		neuron
	FROM filtered_particles
	LEFT JOIN 
	    spacebox.cyberlinks AS c
	ON 
	    filtered_particles.particle = c.particleFrom OR filtered_particles.particle = c.particleTo
), account_debs AS (
	SELECT * FROM spacebox.debs_and_creds
), sum_with_filter as (
	SELECT 
			address,
			height,
			amount,
			sum(amount) over (partition by address, denom order by height) as saldo, 
			denom
	FROM
		account_debs
	WHERE denom = 'millivolt'
), ranked_saldo AS (
	SELECT
		address,
		denom,
		saldo,
		height,
		ROW_NUMBER() OVER (PARTITION BY address, denom
	ORDER BY
		height DESC) AS rn
	FROM sum_with_filter
), filtered_saldo AS (
	SELECT
		address,
		height,
		saldo,
		denom
	FROM
		ranked_saldo
	WHERE
		rn = 1 and saldo != 0
), joined_balances AS (
	SELECT 
		particle,
		particleFrom,
		particleTo,
		neuron,
		saldo as balance
	FROM linked_cyberlinks
	LEFT JOIN filtered_saldo ON linked_cyberlinks.neuron = filtered_saldo.address
), average_vote AS (
	SELECT 
		particle,
		particleFrom,
		particleTo,
		neuron,
		balance / 1000 as balance,
		links,
		balance / links as avg_vote
	FROM joined_balances
	LEFT JOIN (SELECT neuron, count(neuron) as links FROM spacebox.cyberlinks GROUP by neuron) as t ON joined_balances.neuron = t.neuron
), united_particles AS (
	SELECT particleFrom as particle, avg_vote
	FROM average_vote
	UNION ALL
	SELECT particleTo as particle, avg_vote
	FROM average_vote
) 
SELECT particle, sum(avg_vote) as sum_avg_vote 
FROM united_particles
GROUP BY particle
ORDER BY sum_avg_vote DESC
"""