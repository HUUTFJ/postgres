/*
 * src/port/timingsafe_bcmp.c
 *
 *	$OpenBSD: timingsafe_bcmp.c,v 1.3 2015/08/31 02:53:57 guenther Exp $
 */

/*
 * Copyright (c) 2010 Damien Miller.  All rights reserved.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "c.h"

#ifdef USE_SSL
#include <openssl/crypto.h>
#endif

int
timingsafe_bcmp(const void *b1, const void *b2, size_t n)
{
#ifdef USE_SSL
	return CRYPTO_memcmp(b1, b2, n);
#else
	const unsigned char *p1 = b1,
			   *p2 = b2;
	int			ret = 0;

	for (; n > 0; n--)
		ret |= *p1++ ^ *p2++;
	return (ret != 0);
#endif
}
